import csv

with open("ufo.tsv") as tsvfile:
    tsvreader = csv.reader(tsvfile, delimiter="\t")
    for line in tsvreader:
        print("total\t")
        if len(line) != 6:
            print ("badline\t")
            continue
        if len(line[0]) != 0:
            print ("sighted\t")
        if len(line[1]) != 0:
            print ("recorded\t")
        if len(line[2]) != 0:
            print ("location\t")
        if len(line[3]) != 0:
            print ("shape\t")
        if len(line[4]) != 0:
            print ("duration\t")
        if len(line[5]) != 0:
            print ("description\t")
