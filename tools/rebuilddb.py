import os
import sys
import pathlib
import sqlite3
import gzip
import json

regions = ["BR", "EUNE", "EUW", "JP", "KR", "LAN", "LAS", "NA", "OCE", "RU", "TR"]

folder_data = pathlib.Path('.', 'data')

for region in regions:
    print("Processing region {region}".format(region=region))
    folder_region = pathlib.Path(folder_data, region.lower())
    folder_db = pathlib.Path(folder_region, 'database')
    folder_json = pathlib.Path(folder_region, 'json')

    filename_db_matchlist = pathlib.Path(folder_db, "db-discovery-matchlist-" + region.lower() + ".sqlite")
    if not filename_db_matchlist.is_file():
        conn_db_matchlist = sqlite3.connect(str(filename_db_matchlist))
        conn_db_matchlist.execute("CREATE TABLE MatchlistDiscovered (summonerId BIGINT PRIMARY KEY DESC ON CONFLICT IGNORE);")
        conn_db_matchlist.execute("CREATE TABLE MatchlistQueued (summonerId BIGINT PRIMARY KEY DESC ON CONFLICT IGNORE);")
        conn_db_matchlist.execute("CREATE TABLE MatchlistFlushed (summonerId BIGINT PRIMARY KEY DESC ON CONFLICT REPLACE, timestamp  BIGINT);")
        conn_db_matchlist.execute("CREATE TABLE MatchlistError (summonerId BIGINT PRIMARY KEY DESC ON CONFLICT IGNORE, httpCode  INTEGER, timestamp BIGINT);")
        conn_db_matchlist.commit()
    else:
        conn_db_matchlist = sqlite3.connect(str(filename_db_matchlist))

    filename_db_match = pathlib.Path(folder_db, "db-discovery-match-" + region.lower() + ".sqlite")
    if not filename_db_match.is_file():
        conn_db_match = sqlite3.connect(str(filename_db_match))
        conn_db_match.execute("CREATE TABLE MatchDiscovered (matchId BIGINT PRIMARY KEY DESC ON CONFLICT IGNORE);")
        conn_db_match.execute("CREATE TABLE MatchQueued (matchId BIGINT PRIMARY KEY DESC ON CONFLICT IGNORE);")
        conn_db_match.execute("CREATE TABLE MatchFlushed (matchId BIGINT PRIMARY KEY DESC ON CONFLICT IGNORE);")
        conn_db_match.execute("CREATE TABLE MatchError (matchId BIGINT PRIMARY KEY DESC ON CONFLICT IGNORE, httpCode INTEGER, timestamp BIGINT);")
        conn_db_match.commit()
    else:
        conn_db_match = sqlite3.connect(str(filename_db_match))
    
    count = 0
    for file in folder_json.iterdir():
        if file.match('*.json.gz') and file.is_file():
            count += 1

    print("Found {count} matches".format(count=count))

    current = 0
    for file in folder_json.iterdir():
        if file.match('*.json.gz') and file.is_file():
            print("Processing match {current}/{count} ({perc:.0f}%)".format(current=current, count=count, perc=(current*100/count)), end="\r")
            with gzip.GzipFile(file, 'r') as infile:
                match = json.load(infile)

        match_id = match["matchId"]
        participants = match.get("participantIdentities", [])
        players = [participant.get("player", None) for participant in participants]
        summoner_ids = [player.get("summonerId", None) for player in players if player is not None]
        summoners_to_insert = [(int(summoner_id),) for summoner_id in summoner_ids if summoner_id is not None]
        if len(summoners_to_insert):
            conn_db_matchlist.executemany('INSERT INTO "MatchlistDiscovered" VALUES (?);', summoners_to_insert)
            conn_db_matchlist.commit()

        conn_db_match.execute('INSERT INTO "MatchFlushed" VALUES (?);', (match_id,))
        conn_db_match.commit()

        current += 1
    conn_db_match.close()
    conn_db_matchlist.close()
    print("Done")
print("All regions processed")
        



        


