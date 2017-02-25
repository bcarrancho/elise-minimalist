import threading
import time
import logging
import queue
import pathlib
import gzip
import json


class FlushMatchThread(threading.Thread):

    FLUSH_MATCHES = 10
    FLUSH_TIME = 120
    SLEEP_TIME = 5

    def __init__(self, pipe, folder_json):
        logging.debug("Creating flush match thread")

        threading.Thread.__init__(self)
        self.daemon = False
        self.name = "FlushMatch"

        self.folder_json = folder_json
        self.pipe = pipe
        self.last_flush_time = time.time()
        self.flag_exit = threading.Event()
    
    
    def flush(self, matches):
        start_time = time.time()
        count = len(matches)
        if count:
            for match_raw in matches:
                match = json.loads(match_raw)
                match_id = int(match["matchId"])

                # Write JSON file
                filename_json = pathlib.Path(self.folder_json, 'match-' + str(match_id) + '.json.gz')
                if filename_json.exists():
                    logging.warning("Match #{match_id} is already saved in json folder".format(match_id=match_id))
                else:
                    with gzip.GzipFile(str(filename_json), 'w') as outfile:
                        outfile.write(match_raw.encode('utf-8'))
                self.pipe.flush_match.task_done()
                self.pipe.dispatch_match_flush.put(match_id)
                logging.debug("flushed match #{match_id}".format(match_id=match_id))

                # Discover new summoners
                participants = match.get("participants", [])
                players = [participant.get("player", None) for participant in participants]
                summoner_ids = [player.get("summonerId", None) for player in players if player is not None]
                discovery_summoners = [int(summoner_id) for summoner_id in summoner_ids if summoner_id is not None]
                self.pipe.dispatch_matchlist_discover.put(discovery_summoners)

            logging.info("Flushed {count} matches ({t:.0f}s)".format(count=count, t=(time.time() - start_time)))
        self.last_flush_time = time.time()
        
    def get_matches(self):
        matches = []
        while True:
            try:
                m = self.pipe.flush_match.get(False)
                matches.append(m)
            except queue.Empty:
                break
        return matches
    
    def run(self):
        logging.debug("Starting flush match loop")
        while not self.flag_exit.is_set():
            if self.pipe.flush_match.qsize() and ((self.pipe.flush_match.qsize() > FlushMatchThread.FLUSH_MATCHES) or ((time.time() - self.last_flush_time) > FlushMatchThread.FLUSH_TIME)):
                logging.debug("Flushing matches...")
                self.flush(self.get_matches())
            time.sleep(FlushMatchThread.SLEEP_TIME)

        logging.warning("Flush match thread received terminate command, flushing remaining {count} (approx) matches".format(count=self.pipe.flush_match.qsize()))

        self.flush(self.get_matches())
        
        logging.error("Flush match thread ended.")
