import controller

FIELD_DELIMITER = '\x1F'      # ascii unit separator
TUPLE_START = '\x11'          # ascii device control 1
TUPLE_END = '\x12'            # ascii device control 2
BAG_START = '\x13'            # ascii device control 3
BAG_END = '\x14'              # ascii device control 4
MAP_START = '\x05'            # ascii enquiry
MAP_END = '\x06'              # ascii acknowledgement
MAP_KEY = '\x1A'              # ascii substitute
PARAMETER_DELIMITER = '\x1D'  # ascii group separator
NULL_BYTE = '\x16'            # ascii synchronous idle
END_RECORD_DELIM = '\x1E\n'   # ascii record separator + newline

CONTROL_CHARS = set([FIELD_DELIMITER,
                     TUPLE_START, TUPLE_END,
                     BAG_START, BAG_END,
                     MAP_START, MAP_END,
                     PARAMETER_DELIMITER, NULL_BYTE])

TYPE_TUPLE = TUPLE_START
TYPE_BAG = BAG_START
TYPE_MAP = MAP_START

TYPE_CHARARRAY = "C"
TYPE_BYTEARRAY = "A"
TYPE_INTEGER = "I"
TYPE_LONG = "L"
TYPE_FLOAT = "F"
TYPE_DOUBLE = "D"
TYPE_BOOLEAN = "B"

BYTE_TYPE_TUPLE = ord(TYPE_TUPLE)
BYTE_TYPE_BAG = ord(TYPE_BAG)
BYTE_TYPE_MAP = ord(TYPE_MAP)

TUPLE_START_BYTE = ord(TUPLE_START)
BAG_START_BYTE = ord(BAG_START)
MAP_START_BYTE = ord(MAP_START)
TUPLE_END_BYTE = ord(TUPLE_END)
BAG_END_BYTE = ord(BAG_END)
MAP_END_BYTE = ord(MAP_END)
MAP_KEY_BYTE = ord(MAP_KEY)
FIELD_DELIMITER_BYTE = ord(FIELD_DELIMITER)

# -----------------------------------------------------------------------------

test_bytes = (MAP_START + TYPE_CHARARRAY + "A" + MAP_KEY + 
              MAP_START +
              TYPE_CHARARRAY + "B" + MAP_KEY + TYPE_INTEGER + "1" + FIELD_DELIMITER +
              TYPE_CHARARRAY + "C" + MAP_KEY + TYPE_INTEGER + "2" +
              MAP_END +
              FIELD_DELIMITER +
              TYPE_CHARARRAY + "D" + MAP_KEY +
              MAP_START +
              TYPE_CHARARRAY + "E" + MAP_KEY + TYPE_INTEGER + "3" + FIELD_DELIMITER +
              TYPE_CHARARRAY + "F" + MAP_KEY + TYPE_INTEGER + "4" +
              MAP_END + MAP_END)

print controller._deserialize_input(test_bytes, 0, len(test_bytes)-1)
