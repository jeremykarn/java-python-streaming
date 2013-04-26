#! /usr/bin/env python
import sys
import os
import logging

from pig_util import write_user_exception, mortar_logging

FIELD_DELIMITER = ','
TUPLE_START = '('
TUPLE_END = ')'
BAG_START = '{'
BAG_END = '}'
MAP_START = '['
MAP_END = ']'
MAP_KEY = '#'
PARAMETER_DELIMITER = '\t'
PRE_WRAP_DELIM = '|'
POST_WRAP_DELIM = '_'
NULL_BYTE = "-"
END_RECORD_DELIM = '|&\n'

WRAPPED_PARAMETER_DELIMITER = PRE_WRAP_DELIM + PARAMETER_DELIMITER + POST_WRAP_DELIM

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

END_OF_STREAM = TYPE_CHARARRAY + "\x04" + END_RECORD_DELIM
TURN_ON_OUTPUT_CAPTURING = TYPE_CHARARRAY + "TURN_ON_OUTPUT_CAPTURING" + END_RECORD_DELIM
NUM_LINES_OFFSET_TRACE = int(os.environ.get('PYTHON_TRACE_OFFSET', 0))

class PythonStreamingController:
    def __init__(self, profiling_mode=False):
        self.profiling_mode = profiling_mode

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
            return self.get_next_input()

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

    return [_deserialize_input(param, 0, len(param)-1) for param in input_str.split(WRAPPED_PARAMETER_DELIMITER)]

def _deserialize_input(input_str, si, ei):
    if ei - si < 1:
        if ei == si and input_str[0] == TYPE_CHARARRAY:
            return ""
        else:
            return None

    first = input_str[si]
    schema = input_str[si+1] if first == PRE_WRAP_DELIM else first

    if schema == NULL_BYTE:
        return None
    elif schema == TYPE_TUPLE or schema == TYPE_MAP or schema == TYPE_BAG:
        return _deserialize_collection(input_str[si:ei+1])
    else:
        return _cast_scalar(input_str[si], input_str[si+1:ei+1])

def _deserialize_collection(input_str):
    object_stack = []
    map_key_stack = []

    push_to_object_stack = object_stack.append
    push_to_map_key_stack = map_key_stack.append
    pop_from_object_stack = object_stack.pop
    pop_from_map_key_stack = map_key_stack.pop

    index = -1
    depth = 0
    field_start = 0
    just_popped_nested_obj = False
    expecting_map_key = False

    input_len = len(input_str)

    while True:
        if expecting_map_key:
            index = input_str.find(MAP_KEY, index + 1)
            push_to_map_key_stack(unicode(input_str[field_start+1:index], 'utf-8'))
            field_start = index + 1
            expecting_map_key = False

        index = input_str.find(PRE_WRAP_DELIM, index + 1)
        char = input_str[index+1]
        if index == -1:
            raise Exception("Error in method _deserialize_collection() for input string " + input_str)

        # push an object to the stack
        if char == TUPLE_START or char == BAG_START:
            cur_obj = []
            cur_type = char # kind of hacky, but TYPE_COLLECTION == COLLECTION_START
            add_to_cur_obj = cur_obj.append
            push_to_object_stack((cur_obj, cur_type, add_to_cur_obj))

            depth = depth + 1
            field_start = index + 3
        elif char == MAP_START:
            cur_obj = {}
            cur_type = char
            add_to_cur_obj = lambda obj: cur_obj.__setitem__(pop_from_map_key_stack(), obj)
            push_to_object_stack((cur_obj, cur_type, add_to_cur_obj))

            depth = depth + 1
            field_start = index + 3
            expecting_map_key = True
        # pop an object from the stack and add it to its parent object
        elif char == TUPLE_END or char == BAG_END or char == MAP_END:
            # write the last field to the object (unless it's an empty collection)
            if index > field_start:
                add_to_cur_obj(_cast_scalar(input_str[field_start], input_str[field_start+1:index]))

            # pop the object and tuple-ify if necessary
            nested_obj, nested_obj_type, add_to_nested_obj = pop_from_object_stack()
            if nested_obj_type == TYPE_TUPLE:
                nested_obj = tuple(nested_obj)

            depth = depth - 1
            field_start = index + 3
            just_popped_nested_obj = True # flag to ignore next field delim,
                                          # since we do the processing of the object here

            # add the object to its parent if not at root depth
            if depth > 0:
                cur_obj, cur_type, add_to_cur_obj = object_stack[-1]
                add_to_cur_obj(nested_obj)
            else:
                return nested_obj # final exit to the function
        # add a field to the current object
        elif char == FIELD_DELIMITER:
            if just_popped_nested_obj:
                just_popped_nested_obj = False
            else:
                add_to_cur_obj(_cast_scalar(input_str[field_start], input_str[field_start+1:index]))

            expecting_map_key = (cur_type == TYPE_MAP)
            field_start = index + 3

def _cast_scalar(schema, input_str):
    if schema == TYPE_CHARARRAY:
        return unicode(input_str, 'utf-8')
    elif schema == TYPE_BYTEARRAY:
        return bytearray(input_str)
    elif schema == TYPE_INTEGER:
        return int(input_str)
    elif schema == TYPE_LONG:
        return long(input_str)
    elif schema == TYPE_FLOAT or schema == TYPE_DOUBLE:
        return float(input_str)
    elif schema == TYPE_BOOLEAN:
        return input_str == "true"
    elif input_str[0] == NULL_BYTE:
        return None
    else:
        raise Exception("Can't determine type of input: %s" % input_str)

def serialize_output(output, utfEncodeAllFields=False):
    """
    @param utfEncodeStrings - Generally we want to utf encode only strings.  But for
        Maps we utf encode everything because on the Java side we don't know the schema
        for maps so we wouldn't be able to tell which fields were encoded or not.
    """
    output_str = ""
    fd = PRE_WRAP_DELIM + FIELD_DELIMITER + POST_WRAP_DELIM

    if output is None:
        output_str += PRE_WRAP_DELIM + NULL_BYTE + POST_WRAP_DELIM
    elif type(output) == tuple:
        output_str += PRE_WRAP_DELIM + TUPLE_START + POST_WRAP_DELIM
        output_str += fd.join([serialize_output(o, utfEncodeAllFields) for o in output])
        output_str += PRE_WRAP_DELIM + TUPLE_END + POST_WRAP_DELIM
    elif type(output) == list:
        output_str += PRE_WRAP_DELIM + BAG_START + POST_WRAP_DELIM
        output_str += fd.join([serialize_output(o, utfEncodeAllFields) for o in output])
        output_str += PRE_WRAP_DELIM + BAG_END + POST_WRAP_DELIM
    elif type(output) == dict:
        output_str += PRE_WRAP_DELIM + MAP_START + POST_WRAP_DELIM
        output_str += fd.join([ '%s#%s' % (k.encode('utf-8'), serialize_output(v, True)) for k, v in output.iteritems() ])
        output_str += PRE_WRAP_DELIM + MAP_END + POST_WRAP_DELIM
    elif type(output) == bool:
        output_str += "1" if output else "0"
    elif utfEncodeAllFields or isinstance(output, basestring):
        #unicode is necessary in cases where we're encoding non-strings.
        output_str += unicode(output).encode('utf-8')
    else:
        output_str += str(output)

    return output_str

if __name__ == '__main__':
    controller = PythonStreamingController()
    controller.main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4],
                    sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8])
