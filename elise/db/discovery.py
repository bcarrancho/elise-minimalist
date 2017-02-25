import os.path
import sqlite3


import sqlalchemy
import sqlalchemy_utils
import sqlalchemy.orm
import elise.type.common

class DiscoveryDB(object):

    def __init__(self, db_connector):
        _sa_bind_typesystem()
        self.db_connector = db_connector
        self.engine = sqlalchemy.create_engine(self.db_connector)
        if not sqlalchemy_utils.database_exists(self.engine.url):
            sqlalchemy_utils.create_database(self.engine.url)
        elise.type.common.BaseDB.metadata.create_all(self.engine)
        self.session_factory = sqlalchemy.orm.sessionmaker(bind=self.engine)
        self.Session = sqlalchemy.orm.scoped_session(self.session_factory)

    def close(self):
        self.Session.remove()
        self.session_factory.close_all()
        self.engine.dispose()


def _sa_bind_typesystem():
    import elise.type.matchlist
    import elise.type.match
    elise.type.matchlist._sa_bind_all_discovery_db()
    elise.type.match._sa_bind_all()


def init_discovery_db_matchlist(db_filename):
    if not os.path.isfile(db_filename):
        conn = sqlite3.connect(db_filename)
        c = conn.cursor()
        c.execute("CREATE TABLE MatchlistDiscovered (summonerId BIGINT PRIMARY KEY DESC ON CONFLICT IGNORE);")
        c.execute("CREATE TABLE MatchlistQueued (summonerId BIGINT PRIMARY KEY DESC ON CONFLICT IGNORE);")
        c.execute("CREATE TABLE MatchlistFlushed (summonerId BIGINT PRIMARY KEY DESC ON CONFLICT REPLACE, timestamp  BIGINT);")
        c.execute("CREATE TABLE MatchlistError (summonerId BIGINT PRIMARY KEY DESC ON CONFLICT IGNORE, httpCode  INTEGER, timestamp BIGINT);")
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

