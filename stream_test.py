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

def uncollapse_map(m):
    loggg.write("uncollapse_map\n")
    loggg.flush()

    new_m = {}
    for k in m:
        v = m[k]['v']
        loggg.write("%s - Type: %s\n" % (v, type(v)))
        loggg.flush()

        if type(v) == dict:
            v = uncollapse_map(v)
        new_m[k] = v
    return new_m

def avroize_map(m):
    new_map = {}
    for k in m:
        v = m[k]
        if type(v) == dict:
            v = avroize_map(v)
        new_map[k] = {'v':v}
    return new_map

reader = DataStreamReader(stdin, DatumReader())
i = iter(reader)
writer = DataStreamWriter(stdout, DatumWriter(), avro_schema)

loggg.write("Start")
loggg.flush()
while True:
    row = i.next()
    loggg.write("Input: %s\n" % row)
    loggg.flush()

    m = row.get('v')
    loggg.write("Input: %s\n" % m)
    loggg.flush()

    mm = uncollapse_map(m)
    loggg.write("Input: %s\n" % mm)
    loggg.flush()

    c = len(mm)
    d = "%s" % avroize_map(mm)

    writer.append( {"C":c, "D":d} )
    writer.flush()

    #Hack to make Java move along.  Only needed since Java isn't processsing Avro stream
    stdout.write("\n")
    stdout.flush()

    loggg.write("Data written")
    loggg.flush()

