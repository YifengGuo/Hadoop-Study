#!/usr/bin/python
import sys

for line in sys.stdin:
    data = line.split("\t")
    print("total\t")
    if len(data) != 6:
        print ("badline\t")
        continue
    if len(data[0]) != 0:
        print ("sighted\t")
    if len(data[1]) != 0:
        print ("recorded\t")
    if len(data[2]) != 0:
        print ("location\t")
    if len(data[3]) != 0:
        print ("shape\t")
    if len(data[4]) != 0:
        print ("duration\t")
    if len(data[5]) != 0:
        print ("description\t")