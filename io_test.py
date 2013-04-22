#!/usr/bin/python

import sys
import time

start = time.time()
for line in sys.stdin:
    print line
print time.time() - start

start = time.time()
while True:
    input_ = sys.stdin.readline()
    if len(input_) == 0:
        break
    print input_
print time.time() - start
