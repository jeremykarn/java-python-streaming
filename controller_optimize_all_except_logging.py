#! /usr/bin/env python
import sys
import os
import logging

from pig_util import write_user_exception, mortar_logging

# Delimiters are ASCII control characters that are very unlikely to be used in real data
FIELD_DELIMITER = '\x1F'      # ascii unit separator
TUPLE_START = '\x11'          # ascii device control 1
TUPLE_END = '\x12'            # ascii device control 2
BAG_START = '\x13'            # ascii device control 3
BAG_END = '\x14'              # ascii device control 4
MAP_START = '\x05'            # ascii enquiry
MAP_END = '\x06'              # ascii acknowledgement
MAP_KEY = '\x1A'              # ascii substitute
PARAMETER_DELIMITER = '\x1D'  # ascii group separator
NULL_BYTE_CHAR = '\x16'       # ascii synchronous idle
END_RECORD_DELIM = '\x1E\n'   # ascii record separator + newline

TYPE_TUPLE = TUPLE_START
TYPE_BAG = BAG_START
TYPE_MAP = MAP_START

TYPE_BOOLEAN = "B"
TYPE_INTEGER = "I"
TYPE_LONG = "L"
TYPE_FLOAT = "F"
TYPE_DOUBLE = "D"
TYPE_BYTEARRAY = "A"
TYPE_CHARARRAY = "C"

# Byte version of delimiters and types used by _deserialize_collection()
FIELD_DELIMITER_BYTE = ord(FIELD_DELIMITER)
TUPLE_START_BYTE = ord(TUPLE_START)
BAG_START_BYTE = ord(BAG_START)
MAP_START_BYTE = ord(MAP_START)
TUPLE_END_BYTE = ord(TUPLE_END)
BAG_END_BYTE = ord(BAG_END)
MAP_END_BYTE = ord(MAP_END)
NULL_BYTE = ord(NULL_BYTE_CHAR)

MAP_KEY_BYTE = ord(MAP_KEY)
BYTE_TYPE_TUPLE = ord(TYPE_TUPLE)
BYTE_TYPE_BAG = ord(TYPE_BAG)
BYTE_TYPE_MAP = ord(TYPE_MAP)

# \x04 is ascii end of transmission character
END_OF_STREAM = TYPE_CHARARRAY + "\x04" + END_RECORD_DELIM
TURN_ON_OUTPUT_CAPTURING = TYPE_CHARARRAY + "TURN_ON_OUTPUT_CAPTURING" + END_RECORD_DELIM
NUM_LINES_OFFSET_TRACE = int(os.environ.get('PYTHON_TRACE_OFFSET', 0))

class PythonStreamingController:
    def __init__(self):
        self.input_count = 0
        self.next_input_count_to_log = 1

    def main(self,
             module_name, file_path, func_name, cache_path,
             output_stream_path, error_stream_path, log_file_name, is_illustrate_str):
        sys.stdin = os.fdopen(sys.stdin.fileno(), 'rb', 0)

        #Need to ensure that user functions can't write to the streams we use to
        #communicate with pig.
        self.stream_output = os.fdopen(sys.stdout.fileno(), 'wb', 0)
        self.stream_error = os.fdopen(sys.stderr.fileno(), 'wb', 0)

        self.input_stream = sys.stdin
        self.output_stream = open(output_stream_path, 'a')
        sys.stderr = open(error_stream_path, 'w')
        is_illustrate = is_illustrate_str == "true"

        sys.path.append(file_path)
        sys.path.append(cache_path)
        sys.path.append('.')

        logging.basicConfig(filename=log_file_name, format="%(asctime)s %(levelname)s %(message)s", level=mortar_logging.mortar_log_level)
        logging.info("To reduce the amount of information being logged only a small subset of rows are logged at the INFO level.  Call mortar_logging.set_log_level_debug in pig_util to see all rows being processed.")

        input = self.get_next_input()

        try:
            func = __import__(module_name, globals(), locals(), [func_name], -1).__dict__[func_name]
        except:
            #These errors should always be caused by user code.
            write_user_exception(module_name, self.stream_error, NUM_LINES_OFFSET_TRACE)
            self.close_controller(-1)

        if is_illustrate or mortar_logging.mortar_log_level != logging.DEBUG:
            #Only log output for illustrate after we get the flag to capture output.
            sys.stdout = open("/dev/null", 'w')
        else:
            sys.stdout = self.output_stream

        while True:
            try:
                try:
                    self.log_message("Row %s: Serialized Input: %s" % (self.input_count, input))
                    inputs = deserialize_input(input)
                    self.log_message("Row %s: Deserialized Input: %s" % (self.input_count, unicode(inputs)))
                except:
                    #Capture errors where the user passes in bad data.
                    write_user_exception(module_name, self.stream_error, NUM_LINES_OFFSET_TRACE)
                    self.close_controller(-3)

                try:
                    func_output = func(*inputs)
                    self.log_message("Row %s: UDF Output: %s" % (self.input_count, unicode(func_output)))
                except:
                    #These errors should always be caused by user code.
                    write_user_exception(module_name, self.stream_error, NUM_LINES_OFFSET_TRACE)
                    self.close_controller(-2)

                output = serialize_output(func_output)

                self.log_message("Row %s: Serialized Output: %s" % (self.input_count, output))
                if self.input_count == self.next_input_count_to_log:
                    self.update_next_input_count_to_log()

                self.stream_output.write( "%s%s" % (output, END_RECORD_DELIM) )
            except Exception as e:
                #This should only catch internal exceptions with the controller
                #and pig- not with user code.
                import traceback
                traceback.print_exc(file=self.stream_error)
                sys.exit(-3)
            sys.stdout.flush()
            sys.stderr.flush()
            self.stream_output.flush()
            self.stream_error.flush()

            input = self.get_next_input()
            if input == END_OF_STREAM:
                break

    def get_next_input(self):
        input_stream = self.input_stream
        output_stream = self.output_stream

        input = input_stream.readline()

        while input.endswith(END_RECORD_DELIM) == False:
            input += input_stream.readline()

        if input == TURN_ON_OUTPUT_CAPTURING:
            logging.debug("Turned on Output Capturing")
            sys.stdout = output_stream
            return get_next_input(output_stream, input_stream)

        if input == END_OF_STREAM:
            return input

        self.input_count += 1

        return input[:-len(END_RECORD_DELIM)]

    def log_message(self, msg):
        if self.input_count == self.next_input_count_to_log:
            logging.info(msg)
        else:
            logging.debug(msg)

    def update_next_input_count_to_log(self):
        """
        Want to log enough rows that you can see progress being made and see timings without wasting time logging thousands of rows.
        Show first 10 rows, and then the first 5 rows of every order of magnitude (10-15, 100-105, 1000-1005, ...)
        """
        if self.next_input_count_to_log < 10:
            self.next_input_count_to_log = self.next_input_count_to_log + 1
        elif self.next_input_count_to_log % 10 == 5:
            self.next_input_count_to_log = (self.next_input_count_to_log - 5) * 10
        else:
            self.next_input_count_to_log = self.next_input_count_to_log + 1

    def close_controller(self, exit_code):
        sys.stderr.close()
        self.stream_error.write("\n")
        self.stream_error.close()
        sys.stdout.close()
        self.stream_output.write("\n")
        self.stream_output.close()
        sys.exit(exit_code)

def deserialize_input(input_str):
    if len(input_str) == 0:
        return []

    return [_deserialize_input(param, 0, len(param)-1) for param in input_str.split(PARAMETER_DELIMITER)]

def _deserialize_input(input_str, si, ei):
    if ei - si < 1:
        if ei == si and input_str[si] == TYPE_CHARARRAY:
            return ""
        else:
            return None

    schema = input_str[si]

    if schema == NULL_BYTE_CHAR:
        return None
    elif schema == TYPE_TUPLE or schema == TYPE_BAG or schema == TYPE_MAP:
        return _deserialize_collection(input_str[si:ei+1])
    else:
        return _cast_value(input_str[si+1:ei+1], schema)

def _deserialize_collection(input_str):
    type_stack = []
    value_stack = []
    map_key_stack = []

    push_to_type_stack = type_stack.append
    push_to_value_stack = value_stack.append
    push_to_map_key_stack = map_key_stack.append
    pop_from_type_stack = type_stack.pop
    pop_from_value_stack = value_stack.pop
    pop_from_map_key_stack = map_key_stack.pop

    depth = 0
    field_start = 0
    just_popped_nested_obj = False

    for index, byte in enumerate(bytearray(input_str, 'utf-8')):
        if byte < 32 and byte != NULL_BYTE: # if byte is a control character
            # push an object onto the stack
            if byte == TUPLE_START_BYTE or byte == BAG_START_BYTE or byte == MAP_START_BYTE:
                cur_type = byte # kind of hacky, but BYTE_TYPE_COLLECTION == COLLECTION_START_BYTE
                cur_obj = {} if byte == MAP_START_BYTE else []
                push_to_type_stack(cur_type)
                push_to_value_stack(cur_obj)
                depth += 1
            # pop an object from the stack and add it to its parent object
            elif byte == TUPLE_END_BYTE or byte == BAG_END_BYTE or byte == MAP_END_BYTE:
                # write the last field to the object (unless its an empty collection)
                if index > field_start:
                    if cur_type == BYTE_TYPE_MAP:
                        cur_obj[pop_from_map_key_stack()] = _cast_value(input_str[field_start+1:index], input_str[field_start])
                    else:
                        cur_obj.append(_cast_value(input_str[field_start+1:index], input_str[field_start]))

                # pop the object and tuple-ify if necessary
                nested_obj_type = pop_from_type_stack()
                if nested_obj_type == BYTE_TYPE_TUPLE:
                    nested_obj = tuple(pop_from_value_stack())
                else:
                    nested_obj = pop_from_value_stack()
                depth -= 1
                just_popped_nested_obj = True # flag to ignore next field delim,
                                              # since we do the processing of the object here

                # add the object to its parent if not at root depth
                if depth > 0:
                    cur_obj = value_stack[-1]
                    cur_type = type_stack[-1]
                    if cur_type == BYTE_TYPE_MAP:
                        cur_obj[pop_from_map_key_stack()] = nested_obj
                    else:
                        cur_obj.append(nested_obj)
                else:
                    return nested_obj # this is the final exit to the function
            # read a map key
            elif byte == MAP_KEY_BYTE and cur_type == BYTE_TYPE_MAP:
                push_to_map_key_stack(input_str[field_start+1:index])
            # add a field to the current object
            elif byte == FIELD_DELIMITER_BYTE:
                if just_popped_nested_obj:
                    just_popped_nested_obj = False
                elif cur_type == BYTE_TYPE_MAP:
                    cur_obj[pop_from_map_key_stack()] = _cast_value(input_str[field_start+1:index], input_str[field_start])
                else:
                    cur_obj.append(_cast_value(input_str[field_start+1:index], input_str[field_start]))
            else:
                raise Exception("Invalid control byte. " + 
                                "byte = " + str(byte) + ", " +
                                "cur_type = " + str(cur_type) + ".")

            field_start = index + 1

    raise Exception("Internal error in method _deserialize_collection(). Should have terminated already.")

def serialize_output(output, utfEncodeAllFields=False):
    """
    @param utfEncodeStrings - Generally we want to utf encode only strings.  But for
        Maps we utf encode everything because on the Java side we don't know the schema
        for maps so we wouldn't be able to tell which fields were encoded or not.
    """
    output_type = type(output)

    if output is None:
        return NULL_BYTE_CHAR
    elif output_type == tuple:
        return (TUPLE_START +
                FIELD_DELIMITER.join([serialize_output(o, utfEncodeAllFields) for o in output]) +
                TUPLE_END)
    elif output_type == list:
        return (BAG_START +
                FIELD_DELIMITER.join([serialize_output(o, utfEncodeAllFields) for o in output]) +
                BAG_END)
    elif output_type == dict:
        return (MAP_START +
                FIELD_DELIMITER.join(['%s%s%s' % (k.encode('utf-8'), MAP_KEY, serialize_output(v, True)) for k, v in output.iteritems()]) +
                MAP_END)
    elif output_type == bool:
        return "1" if output else "0"
    elif utfEncodeAllFields or output_type == str or output_type == unicode:
        #unicode is necessary in cases where we're encoding non-strings.
        return unicode(output).encode('utf-8')
    else:
        return str(output)

def _cast_value(input_str, schema):
    if not input_str:
        return None

    if schema == TYPE_CHARARRAY:
        return input_str # Pig streaming should be feeding us UTF-8
    elif schema == TYPE_BYTEARRAY:
        return bytearray(input_str, 'utf-8')
    elif schema == TYPE_INTEGER:
        return int(input_str)
    elif schema == TYPE_LONG:
        return long(input_str)
    elif schema == TYPE_FLOAT or schema == TYPE_DOUBLE:
        return float(input_str)
    elif schema == TYPE_BOOLEAN:
        return input_str == "true"
    else:
        raise Exception("Can't determine type of input: %s" % input_str)

if __name__ == '__main__':
    controller = PythonStreamingController()
    controller.main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4],
                    sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8])
