import time
import elise

class MatchlistTransportTokenRaw(object):
    def __init__(self, summoner_id, matchlist):
        self.summonerId = summoner_id
        self.matchlist = matchlist
        self.timestamp = int(time.time())


class MatchlistTransportTokenClean(object):
    def __init__(self, summoner_id, matchlist):
        self.summonerId = summoner_id
        self.timestamp = int(time.time())
        self.matchrefs = [mr.matchId for mr in matchlist.matches if mr.platformId == elise.platformId]


class MatchlistTransportTokenSummoner(object):
    def __init__(self, token):
        self.summonerId = token.summonerId
        self.timestamp = token.timestamp
        self.matchCount = len(token.matchrefs)
