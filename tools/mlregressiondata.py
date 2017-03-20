import sqlite3
import pathlib


regions = ["BR", "EUNE", "EUW", "JP", "KR", "LAN", "LAS", "NA", "OCE", "RU", "TR"]

folder_data = pathlib.Path('.', 'data')

for region in regions:
    print("Processing region {region}".format(region=region))
    folder_region = pathlib.Path(folder_data, region.lower())
    folder_db = pathlib.Path(folder_region, 'database')

    filename_db_matchlist = pathlib.Path(folder_db, "db-discovery-matchlist-" + region.lower() + ".sqlite")
    conn_db_match = sqlite3.connect(str(filename_db_match))

    
