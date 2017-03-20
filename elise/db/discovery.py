import os.path
import sqlite3


def init_discovery_db_matchlist(db_filename):
    if not os.path.isfile(db_filename):
        conn = sqlite3.connect(db_filename)
        c = conn.cursor()
        c.execute("CREATE TABLE MatchlistDiscovered (summonerId BIGINT PRIMARY KEY DESC ON CONFLICT IGNORE);")
        c.execute("CREATE TABLE MatchlistQueued (summonerId BIGINT PRIMARY KEY DESC ON CONFLICT IGNORE);")
        c.execute("CREATE TABLE MatchlistFlushed (summonerId BIGINT PRIMARY KEY DESC ON CONFLICT REPLACE, timestamp BIGINT);")
        c.execute("CREATE TABLE MatchlistError (summonerId BIGINT PRIMARY KEY DESC ON CONFLICT IGNORE, httpCode INTEGER, timestamp BIGINT, matchCount INTEGER);")
        conn.commit()
        conn.close()

def init_discovery_db_match(db_filename):
    if not os.path.isfile(db_filename):
        conn = sqlite3.connect(db_filename)
        c = conn.cursor()
        c.execute("CREATE TABLE MatchDiscovered (matchId BIGINT PRIMARY KEY DESC ON CONFLICT IGNORE);")
        c.execute("CREATE TABLE MatchQueued (matchId BIGINT PRIMARY KEY DESC ON CONFLICT IGNORE);")
        c.execute("CREATE TABLE MatchFlushed (matchId BIGINT PRIMARY KEY DESC ON CONFLICT IGNORE);")
        c.execute("CREATE TABLE MatchError (matchId BIGINT PRIMARY KEY DESC ON CONFLICT IGNORE, httpCode INTEGER, timestamp BIGINT);")
        conn.commit()
        conn.close()

