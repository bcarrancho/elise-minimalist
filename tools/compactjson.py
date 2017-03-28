import os
import sys
import pathlib
import gzip
import json
import argparse


VALID_REGIONS = ("BR", "EUNE", "EUW", "JP", "KR", "LAN", "LAS", "NA", "OCE", "RU", "TR")
folder_data = pathlib.Path('.', 'data')
BATCH_SIZE = 100

def flush_matches(compact_match_list, folder_json_match_compact, file_name, folder_json_match_processed, folder_json_match):
    with gzip.open(str(pathlib.Path(folder_json_match_compact, file_name)), "wt") as f:
        json.dump(compact_match_list, f, ensure_ascii=False)
    for match in compact_match_list:
        filename = "match-" + str(match["matchId"]) + ".json.gz"
        os.rename(pathlib.Path(folder_json_match, filename), pathlib.Path(folder_json_match_processed, filename))

# Parse arguments
try:
    parser = argparse.ArgumentParser(description="Combines single-match json files into one larger text file.")
    parser.add_argument("-r", "--region", help="server region", choices=VALID_REGIONS, required=True, nargs=1, type=str.upper)

except argparse.ArgumentError as e:
    print("Invalid argument: {0}.".format(e.message))
    sys.exit(2)
args = parser.parse_args()

if args.region is None:
    print("Region not set. Set region with -r argument.")
    sys.exit(2)
else:
    print("Region set to {0}.".format(args.region[0].upper()))
    region = args.region[0]

# Set directories
folder_region = pathlib.Path(folder_data, region.lower())
folder_json_match = pathlib.Path(folder_region, 'json-match')
folder_json_match_processed = pathlib.Path(folder_region, 'json-match-processed')
postgresql_connector =  {
    "dbname": "elise-match-json",
    "user": "elise",
    "password": "elise",
    "host": "localhost",
    "port": 5432}

# Count files in json folder
count = 0
for file in folder_json_match.iterdir():
    if file.match('*.json.gz') and file.is_file():
        count += 1

print("Processing {count} matches".format(count=count))

# Find next available number for compact match file
next_number = 1
for file in folder_json_match_compact.iterdir():
    if file.match('*.json.gz') and file.is_file():
        file_name = str(file).split(".")[0]
        file_number = int(file_name.split("-")[-1])
        if file_number >= next_number:
            next_number = file_number + 1

print("Next available file number: {}".format(next_number))

# Main loop
current = 1
batch_json = []
for file in folder_json_match.iterdir():
    if file.match('*.json.gz') and file.is_file():
        print("Processing match {current}/{count} ({perc:.0f}%)".format(current=current, count=count, perc=(current*100/count)))
        compact_json = {}
        with gzip.GzipFile(file, 'r') as infile:
            file_name = str(file).split(".")[0]
            matchId = int(file_name.split("-")[-1])
            match_json = json.load(infile)
            compact_json["matchId"] = matchId
            compact_json["region"] = region.upper()
            compact_json["data"] = match_json
            batch_json.append(compact_json)
            print("matchId: {}, region: {}, data size: {}".format(compact_json["matchId"], compact_json["region"], len(compact_json["data"])))

        if len(batch_json) >= BATCH_SIZE:
            print("flushing {} matches to disk...".format(len(batch_json)))
            filename = "matches-" + region.lower() + "-" + str(next_number) + ".json.gz"
            flush_matches(batch_json, folder_json_match_compact, filename, folder_json_match_processed, folder_json_match)
            next_number += 1
            batch_json = []

        current += 1

print("flushing {} matches to disk...".format(len(batch_json)))
filename = "matches-" + region.lower() + "-" + str(next_number) + ".json.gz"
flush_matches(batch_json, folder_json_match_compact, filename, folder_json_match_processed, folder_json_match)

