loggg = open('python_log', 'w')

from sys import stdin, stdout,stderr
from avro.streaming import DataStreamReader, DataStreamWriter
from avro.io import DatumReader, DatumWriter
from avro.schema import parse

schema_str = '{"namespace": "example.avro",\
               "type": "record",\
               "name": "Udf",\
               "fields": [ {"name": "C", "type": "double"},\
                           {"name": "D", "type": "string"}]}'
avro_schema = parse(schema_str)

reader = DataStreamReader(stdin, DatumReader())
i = iter(reader)
writer = DataStreamWriter(stdout, DatumWriter(), avro_schema)

while True:
    row = i.next()
    a = row.get('A')
    b = row.get('B')
    loggg.write("Input: %s\n" % row)
    loggg.flush()

    c = a + 0.5
    d = "%s-point-five" % b

    writer.append( {"C":c, "D":d} )
    writer.flush()

    #Hack to make Java move along.  Only needed since Java isn't processsing Avro stream
    stdout.write("\n")
    stdout.flush()

    loggg.write("Data written")
    loggg.flush()
