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
import cassiopeia.type.dto.matchlite
import cassiopeia.type.core.matchlite

VALID_REGIONS = ("BR", "EUNE", "EUW", "JP", "KR", "LAN", "LAS", "NA", "OCE", "RU", "TR")
folder_data = pathlib.Path('.', 'data')
BATCH_SIZE = 1000

# Argument parsing
try:
    parser = argparse.ArgumentParser(description="Retrieves full match and summoner info.")
    parser.add_argument("-r", "--region", help="server region", choices=VALID_REGIONS, required=True, nargs=1, type=str.upper)
    parser.add_argument("-n", "--nummatches", help="maximum number of matches to insert into database", nargs=1, type=int)
    parser.add_argument("-t", "--notimeline", help="ignore timeline data", action="store_true", default=False)

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

if args.nummatches is not None:
    nummatches = args.nummatches[0]
else:
    nummatches = None

if args.notimeline:
    cassiopeia.type.api.store.timeline = False

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

if nummatches is not None and count > nummatches:
    count = nummatches

print("Processing {count} matches".format(count=count))

current = 1
for file in folder_json.iterdir():
    if file.match('*.json.gz') and file.is_file():
        print("Processing match {current}/{count} ({perc:.0f}%)".format(current=current, count=count, perc=(current*100/count)), end="\r")
        with gzip.GzipFile(file, 'r') as infile:
            match_json = json.load(infile)
    
        if args.notimeline:
            match_dto = cassiopeia.type.dto.matchlite.MatchDetail(match_json)
            match = cassiopeia.type.core.matchlite.Match(match_dto)
        else:
            match = cassiopeia.type.core.match.Match(cassiopeia.type.dto.match.MatchDetail(match_json))

        db.store(match)
        if nummatches is not None and current >= nummatches:
            break
        current += 1

db.close()
print("Done")
