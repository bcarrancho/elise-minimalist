import threading
import time
import logging
import queue
import gc
import sqlite3

import cassiopeia.dto.leagueapi

import elise
import elise.type.matchlist
from elise.api.retry import *


def query_count(conn, table_name):
    query = 'SELECT COUNT(*) FROM "' + table_name + '"'
    c = conn.execute(query)
    return c.fetchone()[0] if c is not None else None


def empty_table(conn, table_name):
    query = 'DELETE FROM "' + table_name + '"'
    c = conn.execute(query)
    conn.commit()
    return c.rowcount if c is not None else None


class DispatchMatchThread(threading.Thread):

    REQUEST_MATCH_QUEUE_SIZE = 600
    REQUEST_MATCH_QUEUE_THRESHOLD = 500
    FLUSH_MATCHLISTS = 5
    SLEEP_TIME = 5

    def __init__(self, pipe, dbd_match_filename):
        logging.debug("Creating dispatch match thread")
        threading.Thread.__init__(self, daemon=False, name="DispatchMatch")

        self.pipe = pipe
        self.dbd_match_filename = dbd_match_filename
        self.flag_exit = threading.Event()

        logging.debug("Initializing dispatch match thread data structures")
        conn_dbd_match = sqlite3.connect(self.dbd_match_filename)
        count_dbd_match = query_count(conn_dbd_match, "MatchFlushed")
        logging.info("Found {count} flushed matches in the discovery database".format(count=count_dbd_match))
        empty_table(conn_dbd_match, "MatchQueued")
        conn_dbd_match.close()

        logging.debug("Dispatch match thread initialized")

    
    def dispatch(self, conn, n):
        start_time = time.time()
        c = conn.execute('SELECT "matchId" FROM "MatchDiscovered" WHERE "matchId" NOT IN (SELECT "matchId" FROM "MatchQueued") AND "matchId" NOT IN (SELECT "matchId" FROM "MatchFlushed") ORDER BY "matchId" LIMIT (?);', (n,))
        dispatched_matches = [match_id[0] for match_id in c.fetchall()]
        count = len(dispatched_matches)
        if count > 0:
            conn.executemany('INSERT INTO "MatchQueued" VALUES (?);', [(match_id,) for match_id in dispatched_matches])
            conn.commit()
            for match_id in dispatched_matches:
                self.pipe.request_match.put(match_id)
            logging.info("Dispatched {count} new matches ({t:.0f}s)".format(count=count, t=(time.time() - start_time)))
        else:
            logging.debug("Didn't find additional matches; sleeping for 10 seconds")
            time.sleep(10)
        conn.commit()

    
    def flush_matches(self, conn, matches):
        start_time = time.time()
        match_ids = [(match_id,) for match_id in matches]
        conn.execute('BEGIN')
        c1 = conn.executemany('INSERT INTO "MatchFlushed" VALUES (?);', match_ids)
        count_inserted = c1.rowcount
        c2 = conn.executemany('DELETE FROM "MatchQueued" WHERE "matchId" = (?)', match_ids)
        count_deleted = c2.rowcount
        conn.commit()
        count = len(matches)
        for _ in range(count):
            self.pipe.dispatch_match_flush.task_done()
        logging.debug("Dispatch match thread flushed {count} matches ({t:.0f}s)".format(count=count, t=(time.time() - start_time)))
        if count_inserted != count_deleted:
            logging.warning("Count mismatch when moving from MatchDiscovered to MatchQueued (inserted {}, deleted {} matches)!!!".format(count_inserted, count_deleted))
        if count_inserted != len(match_ids):
            logging.warning("Flushed {} match ids to discovery database that were already discovered!!!".format(len(matches) - count_inserted))
    
    
    def flush_matchrefs(self, conn, tokens):
        start_time = time.time()
        count_matchlists = len(tokens)
        matchrefs = []
        for t in tokens:
            matchrefs.extend(t.matchrefs)
        count_matchrefs = len(matchrefs)
        match_ids = [(mr,) for mr in matchrefs]
        c = conn.executemany('INSERT INTO "MatchDiscovered" VALUES (?);', match_ids)
        count_matchrefs_new = c.rowcount
        conn.commit()
        
        for t in tokens:
            self.pipe.flush_matchlist.task_done()
            self.pipe.dispatch_matchlist_flush.put(elise.type.matchlist.MatchlistTransportTokenSummoner(t))

        logging.info("Flushed {count_matchrefs} matchrefs ({count_matchrefs_new} new) from {count_matchlists} summoners ({t:.0f}s)".format(count_matchrefs=count_matchrefs, count_matchrefs_new=count_matchrefs_new, count_matchlists=count_matchlists, t=(time.time() - start_time)))
    
    
    def get_matches(self):
        matches = []
        while True:
            try:
                m = self.pipe.dispatch_match_flush.get(False)
                matches.append(m)
            except queue.Empty:
                break
        return matches


    def get_matchlists(self):
        tokens = []
        while True:
            try:
                t = self.pipe.flush_matchlist.get(False)
                tokens.append(t)
            except queue.Empty:
                break
        return tokens
    

    def run(self):
        logging.debug("Starting dispatch match thread loop")

        last_run_executed = False
        last_run_executing = False
        while not (self.flag_exit.is_set() and last_run_executed):

            # Checks if request queue is filled
            current_queue_len = self.pipe.request_match.qsize()
            if (current_queue_len < DispatchMatchThread.REQUEST_MATCH_QUEUE_THRESHOLD) and not last_run_executing and elise.flag_initialized.is_set():
                matches_to_obtain = DispatchMatchThread.REQUEST_MATCH_QUEUE_SIZE - current_queue_len
                logging.debug("Request matches queue size below threshold, dispatching {count} new matches".format(count=matches_to_obtain))
                conn = sqlite3.connect(self.dbd_match_filename)
                self.dispatch(conn, matches_to_obtain)
                conn.close()
            
            # Checks if there are matches to be flushed
            if self.pipe.dispatch_match_flush.qsize() > 0:
                conn = sqlite3.connect(self.dbd_match_filename)
                self.flush_matches(conn, self.get_matches())
                conn.close()

            # Checks if there are matchrefs to be flushed
            if self.pipe.flush_matchlist.qsize() > DispatchMatchThread.FLUSH_MATCHLISTS:
                conn = sqlite3.connect(self.dbd_match_filename)
                self.flush_matchrefs(conn, self.get_matchlists())
                conn.close()

            time.sleep(DispatchMatchThread.SLEEP_TIME)

            if last_run_executing:
                last_run_executed = True
            if self.flag_exit.is_set():
                if not last_run_executed:
                    last_run_executing = True
                    logging.warning("Dispatch match thread flushing remaining {} matches and matchrefs from {} summoners".format(self.pipe.dispatch_match_flush.qsize(), self.pipe.flush_matchlist.qsize()))
                    time.sleep(5)


        logging.error("Dispatch match thread ended.")


class DispatchMatchlistThread(threading.Thread):

    REQUEST_MATCHLIST_QUEUE_SIZE = 100
    REQUEST_MATCHLIST_QUEUE_THRESHOLD = 80
    SLEEP_TIME = 5

    def __init__(self, pipe, dbd_matchlist_filename, cold_start=False):
        logging.debug("Creating dispatch matchlist thread")
        threading.Thread.__init__(self, name="DispatchMatchlist", daemon=False)

        self.pipe = pipe
        self.dbd_matchlist_filename = dbd_matchlist_filename
        self.cold_start = cold_start
        self.flag_exit = threading.Event()

        conn_dbd_matchlist = sqlite3.connect(self.dbd_matchlist_filename)

        count_dbd_summoners = query_count(conn_dbd_matchlist, "MatchlistDiscovered")
        logging.info("Found {count} summoners in the discovery database".format(count=count_dbd_summoners))
        empty_table(conn_dbd_matchlist, "MatchlistQueued")

        count = self.dispatch(conn_dbd_matchlist, DispatchMatchlistThread.REQUEST_MATCHLIST_QUEUE_SIZE)

        if self.cold_start or not count:
            logging.warning("Pulling master and challenger tiers for seeding")
            summoners = set()
            league = cassiopeia.dto.leagueapi.get_challenger("RANKED_FLEX_SR")
            summoners |= set([int(s.playerOrTeamId) for s in league.entries])
            league = cassiopeia.dto.leagueapi.get_challenger("RANKED_SOLO_5x5")
            summoners |= set([int(s.playerOrTeamId) for s in league.entries])
            league = cassiopeia.dto.leagueapi.get_master("RANKED_FLEX_SR")
            summoners |= set([int(s.playerOrTeamId) for s in league.entries])
            league = cassiopeia.dto.leagueapi.get_master("RANKED_SOLO_5x5")
            summoners |= set([int(s.playerOrTeamId) for s in league.entries])
            dispatched_matchlists = [(summoner_id,) for summoner_id in summoners]
            dispatched_count = len(dispatched_matchlists)
            if dispatched_count:
                conn_dbd_matchlist.executemany('INSERT INTO "MatchlistDiscovered" VALUES (?);', dispatched_matchlists)
                conn_dbd_matchlist.executemany('INSERT INTO "MatchlistQueued" VALUES (?);', dispatched_matchlists)
                conn_dbd_matchlist.commit()
                for s in summoners:
                    self.pipe.request_matchlist.put(s)
                logging.info("Found {count} summoners in challenger and master tiers".format(count=dispatched_count))
            else:
                logging.error("No summoners found for seeding!")
                self.flag_exit.set()
        
        conn_dbd_matchlist.close()
        logging.debug("Dispatch matchlist thread initialized")

    
    def dispatch(self, conn, n):
        start_time = time.time()
        c = conn.execute('SELECT "summonerId" FROM "MatchlistDiscovered" WHERE "summonerId" NOT IN (SELECT "summonerId" FROM "MatchlistQueued") AND "summonerId" NOT IN (SELECT "summonerId" FROM "MatchlistFlushed") ORDER BY "summonerId" LIMIT (?);', (n,))
        dispatched_summoners = [summoner_id[0] for summoner_id in c.fetchall()]
        count = len(dispatched_summoners)
        if count > 0:
            conn.executemany('INSERT INTO "MatchlistQueued" VALUES (?);', [(summoner_id,) for summoner_id in dispatched_summoners])
            conn.commit()
            for summoner_id in dispatched_summoners:
                self.pipe.request_matchlist.put(summoner_id)
            logging.info("Dispatched {count} new summoners ({t:.0f}s)".format(count=count, t=(time.time() - start_time)))
        else:
            logging.debug("Didn't find additional summoners; sleeping for 10 seconds")
            time.sleep(10)
        conn.commit()
        return count

    
    def flush(self, conn, tokens):
        logging.debug("Flushing matchlists...")
        start_time = time.time()
        summoners = [(t.summonerId, t.timestamp) for t in tokens]
        summoner_ids = [(t.summonerId,) for t in tokens]
        conn.execute('BEGIN')
        c1 = conn.executemany('INSERT INTO "MatchlistFlushed" VALUES (?, ?);', summoners)
        count_inserted = c1.rowcount
        c2 = conn.executemany('DELETE FROM "MatchlistQueued" WHERE "summonerId" = (?)', summoner_ids)
        count_deleted = c1.rowcount
        conn.commit()
        count_summoners = len(tokens)
        for _ in range(count_summoners):
            self.pipe.dispatch_matchlist_flush.task_done()
        logging.debug("Dispatch matchlist thread flushed {count} matchlists ({t:.0f}s)".format(count=count_summoners, t=(time.time() - start_time)))
        if count_inserted != count_deleted:
            logging.warning("Count mismatch when moving from MatchlistDiscovered to MatchlistQueued (inserted {}, deleted {} matches)!!!".format(count_inserted, count_deleted))
        if count_inserted != count_summoners:
            logging.warning("Flushed {} summoner ids to discovery database that were already discovered!!!".format(count_summoners - count_inserted))

    
    def discover(self, conn, summoner_lists):
        start_time = time.time()
        count_summoner_list = len(summoner_lists)
        summoner_ids = []
        for summoner_list in summoner_lists:
            summoner_ids.extend(summoner_list)
        count_summoners = len(summoner_ids)
        summoner_ids = [(id_,) for id_ in summoner_ids]
        c = conn.executemany('INSERT INTO "MatchlistDiscovered" VALUES (?);', summoner_ids)
        count_changed = c.rowcount
        conn.commit()
        for _ in range(count_summoner_list):
            self.pipe.dispatch_matchlist_discover.task_done()
        logging.debug("Discovered {} summoners ({} new) in {} lists ({t:.0f}s)".format(count_summoners, count_changed, count_summoner_list, t=(time.time() - start_time)))
    
    
    def get_matches(self):
        matches = []
        while True:
            try:
                m = self.pipe.dispatch_matchlist_discover.get(False)
                matches.append(m)
            except queue.Empty:
                break
        return matches


    def get_matchlists(self):
        tokens = []
        while True:
            try:
                t = self.pipe.dispatch_matchlist_flush.get(False)
                tokens.append(t)
            except queue.Empty:
                break
        return tokens
    

    def run(self):
        logging.debug("Starting dispatch matchlist thread loop")

        last_run_executed = False
        last_run_executing = False
        while not (self.flag_exit.is_set() and last_run_executed):

            # Checks if request queue is filled
            current_queue_len = self.pipe.request_matchlist.qsize()
            if (current_queue_len < DispatchMatchlistThread.REQUEST_MATCHLIST_QUEUE_THRESHOLD) and not last_run_executing and elise.flag_initialized.is_set():
                summoners_to_obtain = DispatchMatchlistThread.REQUEST_MATCHLIST_QUEUE_SIZE - current_queue_len
                logging.debug("Request matchlists queue size below threshold, dispatching {count} new summoners".format(count=summoners_to_obtain))
                conn = sqlite3.connect(self.dbd_matchlist_filename)
                self.dispatch(conn, summoners_to_obtain)
                conn.close()

            
            # Checks if there are matchlists to be flushed
            if (self.pipe.dispatch_matchlist_flush.qsize() > 0) or last_run_executing:
                conn = sqlite3.connect(self.dbd_matchlist_filename)
                self.flush(conn, self.get_matchlists())
                conn.close()

            # Checks if there are summoners to be discovered
            if (self.pipe.dispatch_matchlist_discover.qsize() > 0) or last_run_executing:
                conn = sqlite3.connect(self.dbd_matchlist_filename)
                self.discover(conn, self.get_matches())
                conn.close()

            time.sleep(DispatchMatchlistThread.SLEEP_TIME)

            if last_run_executing:
                last_run_executed = True
            if self.flag_exit.is_set():
                if not last_run_executed:
                    last_run_executing = True
                    logging.warning("Dispatch matchlist thread flushing remaining {} matchlists and discovering remaining {} groups of summoners".format(self.pipe.dispatch_matchlist_flush.qsize(), self.pipe.dispatch_matchlist_discover.qsize()))
                    time.sleep(5)

        logging.error("Dispatch match thread ended.")
