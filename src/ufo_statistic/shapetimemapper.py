#!/usr/bin/python3
import re
import sys

# durations are described by min and sec
pattern = re.compile(r'(\d*)(\s)?((sec)|(min))?')
for line in sys.stdin:
    data = line.split("\t")
    if len(data) == 6:
        shape = data[3].strip()
        duration = data[4].strip().lower()
        if len(shape) != 0 and len(duration) != 0:
            match_duration = re.search(pattern, duration)
            time = re.compile(r'(\s?)(\d*)(\s?)').match(match_duration.group(1))  # match() return Match object, res[0] is digit
            unit = match_duration.group(3)
            if time.group(2).strip() == "":
                continue
            time = int(time.group(2).strip())
            if unit == "min":
                time = time * 60
            print(shape + "\t" + str(time))

