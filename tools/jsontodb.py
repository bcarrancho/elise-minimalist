import os
import sys
import pathlib
import gzip
import json
import argparse

import cassiopeia
import cassiopeia.type.api.store
import cassiopeia.type.dto.match
import cassiopeia.type.core.match

VALID_REGIONS = ("BR", "EUNE", "EUW", "JP", "KR", "LAN", "LAS", "NA", "OCE", "RU", "TR")
folder_data = pathlib.Path('.', 'data')

# Argument parsing
try:
    parser = argparse.ArgumentParser(description="Retrieves full match and summoner info.")
    parser.add_argument("-r", "--region", help="server region for calls to the API", choices=VALID_REGIONS, required=True, nargs=1, type=str.upper)

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

folder_region = pathlib.Path(folder_data, region.lower())
folder_db = pathlib.Path(folder_region, 'database')
folder_json = pathlib.Path(folder_region, 'json')
filename_db = pathlib.Path(folder_db, 'db-main-matches-' + region.lower() + '.sqlite')

db_qualifier = ("sqlite", "", str(filename_db), "", "")

try:
    db = cassiopeia.type.api.store.SQLAlchemyDB(*db_qualifier)
except IOError:
    print("Failed to open database")
    sys.exit(3)

count = 0
for file in folder_json.iterdir():
    if file.match('*.json.gz') and file.is_file():
        count += 1

print("Found {count} matches".format(count=count))

current = 1
for file in folder_json.iterdir():
    if file.match('*.json.gz') and file.is_file():
        print("Processing match {current}/{count} ({perc:.0f}%)".format(current=current, count=count, perc=(current*100/count)), end="\r")
        with gzip.GzipFile(file, 'r') as infile:
            match_json = json.load(infile)
    
        match = cassiopeia.type.core.match.Match(cassiopeia.type.dto.match.MatchDetail(match_json))
        db.store(match)
        current += 1

db.close()
print("Done")
