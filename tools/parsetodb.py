import os
import os.path
import sys
import pathlib
import gzip
import json
import argparse

BATCH_SIZE = 100

def parse(source, destination, notimeline):
    # Checks existance of source file
    if not os.path.isfile(source):
        print("{file_name}: No such file".format(file_name=source))

if __name__ == "__main__":
    # Argument parsing
    try:
        parser = argparse.ArgumentParser(description="Inserts data from JSON database to analysis database.")
        parser.add_argument("-s", "--source", help="source database file", required=True, nargs="+", type=str)
        parser.add_argument("-d", "--destination", help="destination database file", required=True, nargs=1, type=str)
        parser.add_argument("-t", "--notimeline", help="ignore timeline data", action="store_true", default=False)

    except argparse.ArgumentError as e:
        print("Invalid argument: {0}.".format(e.message))
        sys.exit(2)
    args = parser.parse_args()

    for file in args.source:
        parse(file, args.destination, args.notimeline)


