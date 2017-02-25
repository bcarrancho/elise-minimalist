import os
import logging
import queue
import json
import gzip
import time

import cassiopeia
import cassiopeia.baseriotapi

import elise
import elise.thread.request
import elise.type.queuepipe


def flush_matches(matches):
    for m in matches:
        m_json = json.loads(m)
        filename_json = 'match-' + str(m_json["matchId"]) + '.json.gz'
        with gzip.GzipFile(filename_json, 'w') as outfile:
            outfile.write(m.encode('utf-8'))
        pipe.flush_match.task_done()
        logging.info("flushed match #{match_id}".format(match_id=m_json["matchId"]))


def flush_matchlists(match_lists):
    logging.debug("flushing matchlists...")
    for m in match_lists:
        for matchref in m.matchrefs:
            pipe.request_match.put(matchref)
        pipe.flush_matchlist.task_done()


logging.basicConfig(format='%(asctime)s: %(message)s', level=logging.DEBUG, datefmt='%d/%m/%Y %H:%M:%S')

region = "NA"
key = os.environ["DEV_KEY"]
elise.platformId = "NA1"

pipe = elise.type.queuepipe.QueuePipe()

cassiopeia.baseriotapi.set_api_key(key)
cassiopeia.baseriotapi.set_region(region)
cassiopeia.baseriotapi.print_calls(False)

summoners = set()
league = cassiopeia.dto.leagueapi.get_challenger("RANKED_FLEX_SR")
summoners |= set([int(s.playerOrTeamId) for s in league.entries])
league = cassiopeia.dto.leagueapi.get_challenger("RANKED_SOLO_5x5")
summoners |= set([int(s.playerOrTeamId) for s in league.entries])
league = cassiopeia.dto.leagueapi.get_master("RANKED_FLEX_SR")
summoners |= set([int(s.playerOrTeamId) for s in league.entries])
league = cassiopeia.dto.leagueapi.get_master("RANKED_SOLO_5x5")
summoners |= set([int(s.playerOrTeamId) for s in league.entries])

for s in summoners:
    pipe.request_matchlist.put(s)

thread_request = elise.thread.request.RequestThread(pipe)
thread_request.start()

while(True):
    try:
        if pipe.flush_match.qsize() > 0:
            matches = []
            while True:
                try:
                    m = pipe.flush_match.get(False)
                    matches.append(m)
                except queue.Empty:
                    break
            flush_matches(matches)

        if pipe.flush_matchlist.qsize() > 0:
            match_lists = []
            while True:
                try:
                    m = pipe.flush_matchlist.get(False)
                    match_lists.append(m)
                except queue.Empty:
                    break
            flush_matchlists(match_lists)

        time.sleep(5)
                
    except KeyboardInterrupt:
        break
            





