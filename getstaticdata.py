import os
import sys
import pathlib
import gzip
import json
import argparse

import cassiopeia
import cassiopeia.baseriotapi
import cassiopeia.dto.staticdataapi
import elise
from elise.api.retry import *

regions = ["BR", "EUNE", "EUW", "JP", "KR", "LAN", "LAS", "NA", "OCE", "RU", "TR"]
locale = "en_US"
folder_data = pathlib.Path('.', 'data')

# Argument parsing
try:
    parser = argparse.ArgumentParser(description="Retrieves static data.")
    parser.add_argument("-k", "--key", help="developer or production key provided by Riot", nargs=1)

except argparse.ArgumentError as e:
    print("Invalid argument: {0}.".format(e.message))
    sys.exit(2)
args = parser.parse_args()

if args.key is not None:
    logging.info("API key set by command-line argument.")
    key = args.key
else:
    try:
        key = os.environ["DEV_KEY"]
        logging.info("API key set by environment variable DEV_KEY.")
    except KeyError:
        logging.error("API key was not set. Set key with -k argument or set environment variable DEV_KEY.")
        sys.exit(2)

cassiopeia.baseriotapi.set_api_key(key)
cassiopeia.baseriotapi.set_locale(locale)



for region in regions:
    print("Processing region {region}".format(region=region))
    folder_region = pathlib.Path(folder_data, region.lower())
    folder_json_static = pathlib.Path(folder_region, 'json-static')
    cassiopeia.baseriotapi.set_region(region)
    versions = cassiopeia.dto.staticdataapi.get_versions()

    for version in versions:
        print("Processing version {version}".format(version=version))


print("Done")
