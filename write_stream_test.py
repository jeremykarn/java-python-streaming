#!/usr/bin/python

from sys import stdin,stdout
from avro.streaming import DataStreamReader, DataStreamWriter
from avro.io import DatumReader, DatumWriter
from avro.schema import parse


schema_str = '{"namespace": "example.avro",\
               "type": "record",\
               "name": "Udf",\
               "fields": [ {"name": "id", "type": "double"},\
                           {"name": "id_str", "type": "string"}]}'
avro_schema = parse(schema_str)

writer = DataStreamWriter(stdout, DatumWriter(), avro_schema)
for data in [ {"id":1.0, "id_str":"a"},
              {"id":2.0, "id_str":"b"},
              {"id":3.0, "id_str":"c"} ]:
    writer.append(data)
    writer.flush()
#writer.close()
