#!/usr/bin/python3
import sys

current = None
min = 0
max = 0
mean = 0
total = 0
count = 0

for line in sys.stdin:
    shape, time = line.split("\t")
    time = int(time)

    if shape == current:
        count = count + 1
        total = total + time
        if time < min:
            min = time
        if time > max:
            max = time
    else:
        if current is not None:
            print(current + "\t" + str(min) + " " + str(max) + " " + str(total / count))
        current = shape
        count = 1
        total = time
        min = time
        max = time

print(current + "\t" + str(min) + " " + str(max) + " " + str(total / count))  # print last line