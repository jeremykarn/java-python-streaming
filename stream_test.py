loggg = open('python_log', 'w')

from avro.streaming import DataStreamReader
from avro.streaming import DataStreamWriter
from avro.io import DatumReader, DatumWriter
from avro.schema import parse
from sys import stdin, stdout, stderr
import io

schema_str1 = """
{ "namespace": "example.avro",
  "type": "record",
  "name": "UDF",
  "fields": [
      {"name": "A1", "type": "double"},
      {"name": "A2", "type": "string"},
      {"name": "A3", "type": "double"},
      {"name": "A4", "type": "string"},
      {"name": "A5", "type": "double"},
      {"name": "A6", "type": "string"},
      {"name": "A7", "type": "double"},
      {"name": "A8", "type": "string"},
      {"name": "A9", "type": "double"},
      {"name": "A10", "type": "string"}
      ]}
"""
schema_str2 = """
{
  "type": "record",
  "name": "pseudojson",
  "fields": [
    {
      "name": "v",
      "type": [
          "long",
          "double",
          "string",
          "boolean",
          "null",
          {"type": "array", "items": "pseudojson"},
          {"type": "map", "values": "pseudojson"}
      ]
    }
  ]
}
"""
schema_str3 = """
{
  "type": "record",
  "name": "pseudojson",
  "fields": [
    {
      "name": "v",
      "type": [
          {"type": "map", "values": "string"}
      ]
    }
  ]
}
"""
schema_str = schema_str3
avro_schema = parse(schema_str)

def log(m):
    #loggg.write(m)
    #loggg.flush()
    pass

def uncollapse_map(m):
    new_m = {}
    for k in m:
        v = m[k]['v']
        #loggg.write("%s - Type: %s\n" % (v, type(v)))
        #loggg.flush()

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

def main():
    reader = DataStreamReader(stdin, DatumReader())
    i = iter(reader)
    writer = DataStreamWriter(stdout, DatumWriter(), avro_schema)

#loggg.write("Start")
#loggg.flush()
    count = 0
    while True:
        row = i.next()
        #log("Input: %s" % (row))
        #loggg.write("Input: %s\n" % row)
        #loggg.flush()

        m = row.get('v')
        #loggg.write("Input: %s\n" % m)
        #loggg.flush()

        #Only need to uncollapse if generic map.
        #mm = uncollapse_map(m)
        #v = avroize_map(mm)

        #writer.append({'v':v})
        #writer.flush()
        writer.append({'v':m})
        writer.flush()
        #writer.append(row)
        #writer.flush()
        count += 1
        if count == 10000:
            return

        #loggg.write("Data written")
        #loggg.flush()

import cProfile
import pstats
pr = cProfile.Profile()
pr.enable()
main()
pr.disable()
pr.dump_stats('avro.profile')
