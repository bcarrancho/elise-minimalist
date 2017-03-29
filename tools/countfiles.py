import os
import sys
import pathlib


regions = ["BR", "EUNE", "EUW", "JP", "KR", "LAN", "LAS", "NA", "OCE", "RU", "TR"]
folder_data = pathlib.Path('.', 'data')

for region in regions:
    print("Processing region {}".format(region.upper()))

    # Set directories
    folder_region = pathlib.Path(folder_data, region.lower())
    folder_json_match = pathlib.Path(folder_region, 'json-match')
    folder_json_match_processed = pathlib.Path(folder_region, 'json-match-processed')

    # Count files in json folder
    count = 0
    for file in folder_json_match.iterdir():
        if file.match('*.json.gz') and file.is_file():
            count += 1
    print("Files in Match JSON folder: {}".format(count))

    # Count files in json processed folder
    count = 0
    for file in folder_json_match_processed.iterdir():
        if file.match('*.json.gz') and file.is_file():
            count += 1
    print("Files in Processed Match JSON folder: {}".format(count))

print("Done")