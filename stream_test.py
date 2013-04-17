from sys import stdin, stdout,stderr
from avro.streaming import DataStreamReader, DataStreamWriter
from avro.io import DatumReader, DatumWriter
from avro.schema import parse

schema_str = """
{ "namespace": "example.avro",
  "type": "record",
  "name": "UDF",
  "fields": [
      {"name": "DBL", "type": "double"},
      {"name": "STR", "type": "string"}]}
"""
avro_schema = parse(schema_str)

reader = DataStreamReader(stdin, DatumReader())
i = iter(reader)
writer = DataStreamWriter(stdout, DatumWriter(), avro_schema)

with open('stream_test.log', 'w') as log:
    while True:
        row = i.next()
        
        log.write("Input: %s\n" % row)
        log.flush()

        d = row.get('DBL') + 0.5
        s = row.get('STR').upper()
        out = { "DBL": d, "STR": s }

        writer.append(out)
        writer.flush()

        log.write("Output: %s\n" % out)
        log.flush()
