#! /usr/bin/env python
import sys
import os
import logging

from pig_util import write_user_exception

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

BAG = 0
TUPLE = 1
MAP = 2

TYPE_BOOLEAN = "B"
TYPE_INTEGER = "I"
TYPE_LONG = "L"
TYPE_FLOAT = "F"
TYPE_DOUBLE = "D"
TYPE_BYTEARRAY = "A"
TYPE_CHARARRAY = "C"

TURN_ON_OUTPUT_CAPTURING = "CTURN_ON_OUTPUT_CAPTURING|&\n"
NUM_LINES_OFFSET_TRACE = int(os.environ.get('PYTHON_TRACE_OFFSET', 0))

input_count = 0
logg = open('old_python', 'w')

def log(m):
    logg.write("%s\n" % m)
    logg.flush()

def main():
    log("Start")
    #Need to ensure that user functions can't write to the streams we use to
    #communicate with pig.
    stream_output = os.fdopen(sys.stdout.fileno(), 'wb', 0)
    stream_err_output = os.fdopen(sys.stderr.fileno(), 'wb', 0)

    input = get_next_input(sys.stdout, sys.stdin)
    next_input_count_to_log = 1

    while True:
        try:
            log("Input: %s" % input)
            inputs = deserialize_input(input)

            log("Inputs: %s" % inputs)
            func_output = inputs

            log("func output: %s" % func_output)
            output = serialize_output(func_output)

            log("output: %s" % output)
            next_input_count_to_log = get_next_input_count_to_log(input_count, next_input_count_to_log)
            stream_output.write( "%s%s" % (output, END_RECORD_DELIM) )

        except Exception as e:
            #This should only catch internal exceptions with the controller
            #and pig- not with user code.
            import traceback
            traceback.print_exc(file=stream_err_output)
            sys.exit(-3)
        sys.stdout.flush()
        sys.stderr.flush()
        stream_output.flush()
        stream_err_output.flush()

        input = get_next_input(sys.stdout, sys.stdin)

def get_next_input(output_stream, input_stream):
    """
    TODO: It's really ugly that we pass the output_stream to this function just in case we happen to get
    the capture output flag and that we have the global input_count.  It's probably time we refactor a lot
    of this file into a sensible class with appropriate member variables.
    """
    global input_count
    input = input_stream.readline()

    while input.endswith(END_RECORD_DELIM) == False:
    	input += input_stream.readline()

    if input == TURN_ON_OUTPUT_CAPTURING:
        logging.debug("Turned on Output Capturing")
        sys.stdout = output_stream
        return get_next_input(output_stream, input_stream)
    input_count += 1

    return input[:-len(END_RECORD_DELIM)]


def log_message(msg, input_count, next_input_count_to_log):
    if input_count == next_input_count_to_log:
        logging.info(msg)
    else:
        logging.debug(msg)

def get_next_input_count_to_log(input_count, next_input_count_to_log):
    """
    Want to log enough rows that you can see progress being made and see timings without wasting time logging thousands of rows.
    Show first 10 rows, and then the first 5 rows of every order of magnitude (10-15, 100-105, 1000-1005, ...)
    """
    if input_count != next_input_count_to_log:
        return next_input_count_to_log
    elif next_input_count_to_log < 10:
        return next_input_count_to_log + 1
    elif next_input_count_to_log % 10 == 5:
        return (next_input_count_to_log - 5) * 10
    else:
        return next_input_count_to_log + 1

def close_controller(exit_code, stream_output, stream_err):
    sys.stderr.close()
    stream_err.write("\n")
    stream_err.close()
    sys.stdout.close()
    stream_output.write("\n")
    stream_output.close()
    sys.exit(exit_code)

def deserialize_input(input):
    if len(input) == 0:
        return []

    pd = PRE_WRAP_DELIM + PARAMETER_DELIMITER + POST_WRAP_DELIM
    params = input.split(pd)
    input_result = []

    for i in range(len(params)):
        input_result.append(_deserialize_input(params[i], 0, len(params[i]) - 1))
    return input_result

def _deserialize_input(input, si, ei):
    if ei >= si + 2 and input[si+1] == NULL_BYTE\
                    and input[si] == PRE_WRAP_DELIM\
                    and input[si+2] == POST_WRAP_DELIM:
        return None
    schema = _get_schema(input, si, ei)
    if schema == 'bag':
        return _deserialize_collection(input, BAG, si+3, ei-3)
    elif schema == 'tuple':
        return _deserialize_collection(input,  TUPLE, si+3, ei-3)
    elif schema == "map":
        return _deserialize_collection(input, MAP, si+3, ei-3)
    else:
        return cast_val(input, schema, si+1, ei)

def _get_schema(input, si, ei):
    first = input[si]
    if first == PRE_WRAP_DELIM:
        second = input[si+1]
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
        raise Exception("Can't determine type of input: %s" % input[si:ei+1])


def _deserialize_collection(input, return_type, si, ei):
    start = si
    index = si
    depth = 0
    result = []
    field_count = 0
    res_append = result.append #For Improved Perf

    key = None
    pre = None
    mid = None
    while index <= ei:
        if len(input) > 2 and index > si + 1:
            pre = input[index - 2]
        if len(input) > 1 and index > si:
            mid = input[index - 1]
        post = input[index]

        if return_type == MAP and post == MAP_KEY and not key:
            key = unicode(input[start+1:index], 'utf-8')
            start = index + 1

        if pre == PRE_WRAP_DELIM and post == POST_WRAP_DELIM:
            if mid == BAG_START or mid == TUPLE_START or mid == MAP_START:
                depth += 1
            elif mid == BAG_END or mid == TUPLE_END or mid == MAP_END:
                depth -= 1

        if depth == 0 and ( index == ei or
                            ( pre == PRE_WRAP_DELIM and
                              mid == FIELD_DELIMITER and
                             post == POST_WRAP_DELIM ) ):
            if index < ei:
                end_index = index - 3
            else:
                end_index = index

            if return_type == TUPLE:
                res_append(_deserialize_input(input, start, end_index))
            elif return_type == MAP:
                res_append( (key, _deserialize_input(input, start, end_index)))
                key = None
            else:
                res_append(_deserialize_input(input, start, end_index))
            field_count += 1
            start = index + 1
        index += 1
    if return_type == TUPLE:
        return tuple(result)
    elif return_type == MAP:
        return dict(result)
    else:
        return result


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
    main()
