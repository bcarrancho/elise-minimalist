import os
import sys
import pathlib
import gzip
import json

import cassiopeia
import cassiopeia.type.api.store
import cassiopeia.type.dto.match
import cassiopeia.type.core.match

regions = ["BR", "EUNE", "EUW", "JP", "KR", "LAN", "LAS", "NA", "OCE", "RU", "TR"]

folder_data = pathlib.Path('.', 'data')

for region in regions:
    print("Processing region {region}".format(region=region))
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
print("All regions processed")
