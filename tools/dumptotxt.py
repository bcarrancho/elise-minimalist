import os
import sys
import pathlib
import sqlite3

regions = ["BR", "EUNE", "EUW", "JP", "KR", "LAN", "LAS", "NA", "OCE", "RU", "TR"]
folder_data = pathlib.Path('.', 'data')

for region in regions:
    print("processing region {}".format(region))
    folder_region = pathlib.Path(folder_data, region.lower())
    folder_db = pathlib.Path(folder_region, 'database')
    filename_db = pathlib.Path(folder_db, 'db-discovery-match-' + region.lower() + '.sqlite')

    conn = sqlite3.connect(str(filename_db))
    result = conn.execute("SELECT matchId FROM MatchDiscovered ORDER BY matchId DESC LIMIT 1000000")
    with open("matches-" + region.lower() + ".txt", "w") as f:
        for match_id in result:
            f.write(str(match_id[0]) + "\n")

    conn.close()



