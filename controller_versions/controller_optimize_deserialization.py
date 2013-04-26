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

    pd = PRE_WRAP_DELIM + PARAMETER_DELIMITER + POST_WRAP_DELIM
    params = input_str.split(pd)
    input_result = []

    for i in range(len(params)):
        input_result.append(_deserialize_input(params[i], 0, len(params[i]) - 1))
    return input_result

def _deserialize_input(input_str, si, ei):
    if ei >= si + 2 and input_str[si+1] == NULL_BYTE\
                    and input_str[si] == PRE_WRAP_DELIM\
                    and input_str[si+2] == POST_WRAP_DELIM:
        return None
    schema = _get_schema(input_str, si, ei)
    if schema == 'bag':
        return _deserialize_collection(input_str[si+3:ei-2], TYPE_BAG)
    elif schema == 'tuple':
        return _deserialize_collection(input_str[si+3:ei-2], TYPE_TUPLE)
    elif schema == "map":
        return _deserialize_collection(input_str[si+3:ei-2], TYPE_MAP)
    else:
        return cast_val(input_str, schema, si+1, ei)

def _get_schema(input_str, si, ei):
    first = input_str[si]
    if first == PRE_WRAP_DELIM:
        second = input_str[si+1]
        if second == BAG_START:
            return 'bag'
        elif second == TUPLE_START:
            return 'tuple'
        elif second == MAP_START:
            return 'map'
        elif second == NULL_BYTE:
            return 'null'
    elif first == TYPE_BYTEARRAY:
        return 'bytearray'
    elif first == TYPE_BOOLEAN:
        return 'boolean'
    elif first == TYPE_CHARARRAY:
        return 'chararray'
    elif first == TYPE_DOUBLE:
        return 'double'
    elif first == TYPE_FLOAT:
        return 'float'
    elif first == TYPE_INTEGER:
        return 'int'
    elif first == TYPE_LONG:
        return 'long'
    else:
        raise Exception("Can't determine type of input: %s" % input_str[si:ei+1])

def _deserialize_collection(input_str, return_type):
    list_result = []
    append_to_list_result = list_result.append
    dict_result = {}

    input_len = len(input_str)
    end_index = input_len - 3

    index = 0
    field_start = 0
    depth = 0

    key = None

    while True:
        if index >= end_index:
            if return_type == TYPE_MAP:
                dict_result[key] = _deserialize_input(input_str, value_start, input_len - 1)
            else:
                append_to_list_result(_deserialize_input(input_str, field_start, input_len - 1))
            break

        if return_type == TYPE_MAP and not key:
            key_index = input_str.find(MAP_KEY, index)
            key = unicode(input_str[index+1:key_index], 'utf-8')
            index = key_index + 1
            value_start = key_index + 1
            continue

        if not (input_str[index] == PRE_WRAP_DELIM and input_str[index+2] == POST_WRAP_DELIM):
            prewrap_index = input_str.find(PRE_WRAP_DELIM, index+1)
            index = (prewrap_index if prewrap_index != -1 else end_index)

        mid = input_str[index+1]

        if mid == BAG_START or mid == TUPLE_START or mid == MAP_START:
            depth += 1
        elif mid == BAG_END or mid == TUPLE_END or mid == MAP_END:
            depth -= 1
        elif depth == 0 and mid == FIELD_DELIMITER:
            if return_type == TYPE_MAP:
                dict_result[key] = _deserialize_input(input_str, value_start, index - 1)
                key = None
            else:
                append_to_list_result(_deserialize_input(input_str, field_start, index - 1))
            field_start = index + 3
        
        index += 3

    if return_type == TYPE_MAP:
        return dict_result
    elif return_type == TYPE_TUPLE:
        return tuple(list_result)
    else:
        return list_result

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

def cast_val(val, type, si, ei):
    """
    Cast val to the python equivalent of the Pig type type.

    @param val: Input string
    @param type: pig type of val.
    """
    if type == 'chararray':
        return unicode(val[si:ei+1], 'utf-8')
    elif type == 'bytearray':
        return bytearray(val[si:ei+1])
    elif si > ei or type == 'null':
        return None
    elif type == 'long':
        return long(val[si:ei+1])
    elif type == 'int':
        return int(val[si:ei+1])
    elif type == 'float' or type == 'double':
        return float(val[si:ei+1])
    elif type == 'boolean':
        return val[si:ei+1] == "true"
    else:
        raise Exception("Invalid type: %s" % type)

if __name__ == '__main__':
    controller = PythonStreamingController()
    controller.main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4],
                    sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8])
