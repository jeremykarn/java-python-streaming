#!/usr/bin/python

from random import random

import avro.schema
from avro.io import DatumReader, DatumWriter
from avro.streaming import DataStreamWriter

schema = avro.schema.parse(open("pseudojson.avsc").read())

writer = DataStreamWriter(open('test_data.avro', 'w'), DatumWriter(), schema)
for i in range(10000):
    # test variable record length
    if random() < 0.5:
        arr = [{'value': i+1}]
        map_ = {'a': {'value': i+2}}
    else:
        arr = [{'value': i+1}, {'value': i+2}]
        map_ = {'a': {'value': i+3}, 'b': {'value': i+4}}

    writer.append({'value': {
        'scalar': {'value': i},
        'arr': {'value': arr},
        'map': {'value': map_}
    }})
writer.close()
