import threading
import time
import logging
import enum
import queue
import json

import cassiopeia.dto.matchapi
import cassiopeia.dto.matchlistapi
import cassiopeia.type.api.exception

import elise
import elise.type.matchlist
from elise.api.retry import *

class RequestMode(enum.Enum):
    match = 1
    matchlist = 2


class RequestThread(threading.Thread):

    MATCH_BATCH = 10
    MATCHLIST_BATCH = 20
    MATCHLIST_MAX_ELAPSED_DAYS = 0

    def __init__(self, pipe, include_timeline=True, num_matches=0, begin_index=0, begin_time=0, end_time=0, champion_ids=None, ranked_queues=None, seasons=None, cold_start=False):
        logging.debug("Creating request thread")

        threading.Thread.__init__(self)
        self.daemon = False
        self.name = "Request"

        self.pipe = pipe
        self.match_counter = 0
        self.matchlist_counter = 0
        self.mode = RequestMode.matchlist if cold_start else RequestMode.match
        self.include_timeline = include_timeline

        self.flag_exit = threading.Event()
        self.last_request_time = time.time()

    def run(self):
        logging.debug("Starting request thread loop")
        while not self.flag_exit.is_set():
            if self.mode == RequestMode.match:
                try:
                    match_id = self.pipe.request_match.get(True, 1)
                except queue.Empty:
                    logging.debug("Matches queue empty, switching to matchlist mode")
                    self.mode = RequestMode.matchlist
                    self.matches_counter = 0
                    continue
                try:
                    self.current_request_time = time.time()
                    request_overhead = self.current_request_time - self.last_request_time
                    start_time = time.time()
                    match_json = cassiopeia.dto.matchapi.get_match(match_id, self.include_timeline)
                    request_time = (time.time() - start_time)
                    self.last_request_time = time.time()
                    self.pipe.request_match.task_done()
                except cassiopeia.type.api.exception.APIError as e:
                    logging.warning("HTTP error {error} while trying to get match #{match_id}".format(error=e.error_code, match_id=match_id))
                    self.pipe.request_match.task_done()
                    continue
                match_age = (time.time() - (int(json.loads(match_json)["matchCreation"]) / 1000)) / (60 * 60 * 24)
                logging.info("Retrieved match #{match_id} (req: {request_time:.1f}s / ovh: {request_overhead:.1f}s / age: {age:.1f}d)".format(match_id=match_id, request_time=request_time, request_overhead=request_overhead, age=match_age))
                if match_json is not None:
                    self.pipe.flush_match.put(match_json)
                    self.match_counter += 1
                if self.match_counter >= RequestThread.MATCH_BATCH:
                    logging.debug("Reached end of matches batch, switching to matchlist mode")
                    self.mode = RequestMode.matchlist
                    self.match_counter = 0

            elif self.mode == RequestMode.matchlist:
                try:
                    summoner_id = self.pipe.request_matchlist.get(True, 1)
                except queue.Empty:
                    if not elise.flag_initialized.is_set():
                        logging.info("End of seeding run, switching to match retrieve mode")
                        elise.flag_initialized.set()
                    else:
                        logging.debug("Summoners queue empty, switching to match mode")
                    self.mode = RequestMode.match
                    self.matchlist_counter = 0
                    continue
                try:
                    self.current_request_time = time.time()
                    request_overhead = self.current_request_time - self.last_request_time
                    start_time = time.time()
                    if RequestThread.MATCHLIST_MAX_ELAPSED_DAYS:
                        begin_time = int((time.time() * 1000) - (RequestThread.MATCHLIST_MAX_ELAPSED_DAYS * 1000 * 86400))
                    else:
                        begin_time = 0
                    match_list = cassiopeia.dto.matchlistapi.get_match_list(summoner_id, begin_time=begin_time)
                    request_time = (time.time() - start_time)
                    self.last_request_time = time.time()
                    self.pipe.request_matchlist.task_done()
                except cassiopeia.type.api.exception.APIError as e:
                    logging.warning("HTTP error {error} while trying to get matchlist for summoner #{summoner_id}".format(error=e.error_code, summoner_id=summoner_id))
                    self.pipe.request_matchlist.task_done()
                    continue
                logging.info("Retrieved matchlist of summoner #{summoner_id} (req: {request_time:.1f}s / ovh: {request_overhead:.1f}s)".format(summoner_id=summoner_id, request_time=request_time, request_overhead=request_overhead))

                self.pipe.flush_matchlist.put(elise.type.matchlist.MatchlistTransportTokenClean(summoner_id, match_list))

                self.matchlist_counter += 1
                if (self.matchlist_counter >= RequestThread.MATCHLIST_BATCH):
                    logging.debug("Reached end of summoners batch, switching to match mode")
                    self.mode = RequestMode.match
                    self.matchlist_counter = 0

        logging.error("Request thread ended.")