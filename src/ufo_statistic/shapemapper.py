#!/usr/bin/python
import sys

for line in sys.stdin:
    data = line.split("\t")
    if len(data) == 6:
        shape = data[3].strip()
        if len(shape) != 0:
            print shape + "\t"
