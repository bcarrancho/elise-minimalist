import psycopg2
import sqlite3


postgresql_query = """
    CREATE TABLE public."MatchJSON"
    (
        id bigserial,
        region character varying(4) NOT NULL,
        "matchId" bigint NOT NULL,
        "timestampRetrieved" bigint,
        "matchJSON" json NOT NULL,
        PRIMARY KEY (id),
        UNIQUE (region, "matchId")
    )
    WITH (
        OIDS = FALSE
    )
    TABLESPACE pg_default;

    ALTER TABLE public."MatchJSON"
        OWNER to elise;        
"""


def initialize_postgresql(db_connector):
    pass


def initialize_sqlite(filename):
    pass