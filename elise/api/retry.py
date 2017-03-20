import time
import logging

import cassiopeia.dto.requests
import future.backports.urllib.error
import cassiopeia.type.api.exception
import urllib.error

from cassiopeia.dto.leagueapi import *
from cassiopeia.dto.matchapi import *
from cassiopeia.dto.matchlistapi import *
from cassiopeia.dto.staticdataapi import *

exit = False

def auto_retry(api_call_method):
    """
    A decorator to automatically retry 500s (Service Unavailable) and skip 400s (Bad Request) or 404s (Not Found).
    """
    def call_wrapper(*args, **kwargs):
        n = 0
        while True:
            try:
                result = api_call_method(*args, **kwargs)
                n = 0
                cassiopeia.dto.requests.successful = True
                return result
                
            except cassiopeia.type.api.exception.APIError as error:
                if error.error_code in [500, 503, 504]:
                    n += 1
                    if n == 1:
                        logging.info("Warning: API error with code {} - service problem. Retrying.".format(error.error_code))
                        continue
                    if n == 2:
                        logging.info("Warning: API error with code {} - service problem. Waiting 10 seconds and retrying.".format(error.error_code))
                        if exit:
                            break
                        time.sleep(10)
                        continue
                    if n >= 3:
                        logging.warning("Warning: API error with code {} - service problem. Waiting 20 seconds and retrying.".format(error.error_code))
                        if exit:
                            break
                        time.sleep(20)
                        continue
                elif error.error_code in [400, 401, 404]:
                    raise error
            except future.backports.urllib.error.URLError as error:
                print("Warning: URL error with reason '{}'. Waiting 10 seconds and retrying.".format(error.reason))
                if exit:
                    break
                time.sleep(10)
                continue
            except urllib.error.URLError as error:
                print("Warning: URL error with reason '{}'. Waiting 10 seconds and retrying.".format(error.reason))
                if exit:
                    break
                time.sleep(10)
                continue

    return call_wrapper


cassiopeia.dto.leagueapi.get_leagues_by_summoner = auto_retry(cassiopeia.dto.leagueapi.get_leagues_by_summoner)
cassiopeia.dto.leagueapi.get_league_entries_by_summoner = auto_retry(cassiopeia.dto.leagueapi.get_league_entries_by_summoner)
cassiopeia.dto.leagueapi.get_leagues_by_team = auto_retry(cassiopeia.dto.leagueapi.get_leagues_by_team)
cassiopeia.dto.leagueapi.get_league_entries_by_team = auto_retry(cassiopeia.dto.leagueapi.get_league_entries_by_team)
cassiopeia.dto.leagueapi.get_challenger = auto_retry(cassiopeia.dto.leagueapi.get_challenger)
cassiopeia.dto.leagueapi.get_master = auto_retry(cassiopeia.dto.leagueapi.get_master)
cassiopeia.dto.matchapi.get_match = auto_retry(cassiopeia.dto.matchapi.get_match)
cassiopeia.dto.matchapi.get_tournament_match_ids = auto_retry(cassiopeia.dto.matchapi.get_tournament_match_ids)
cassiopeia.dto.matchlistapi.get_match_list = auto_retry(cassiopeia.dto.matchlistapi.get_match_list)
cassiopeia.dto.staticdataapi.get_champion = auto_retry(cassiopeia.dto.staticdataapi.get_champion)
cassiopeia.dto.staticdataapi.get_champions = auto_retry(cassiopeia.dto.staticdataapi.get_champions)
cassiopeia.dto.staticdataapi.get_item = auto_retry(cassiopeia.dto.staticdataapi.get_item)
cassiopeia.dto.staticdataapi.get_items = auto_retry(cassiopeia.dto.staticdataapi.get_items)
cassiopeia.dto.staticdataapi.get_language_strings = auto_retry(cassiopeia.dto.staticdataapi.get_language_strings)
cassiopeia.dto.staticdataapi.get_languages = auto_retry(cassiopeia.dto.staticdataapi.get_languages)
cassiopeia.dto.staticdataapi.get_maps = auto_retry(cassiopeia.dto.staticdataapi.get_maps)
cassiopeia.dto.staticdataapi.get_masteries = auto_retry(cassiopeia.dto.staticdataapi.get_masteries)
cassiopeia.dto.staticdataapi.get_mastery = auto_retry(cassiopeia.dto.staticdataapi.get_mastery)
cassiopeia.dto.staticdataapi.get_realm = auto_retry(cassiopeia.dto.staticdataapi.get_realm)
cassiopeia.dto.staticdataapi.get_rune = auto_retry(cassiopeia.dto.staticdataapi.get_rune)
cassiopeia.dto.staticdataapi.get_runes = auto_retry(cassiopeia.dto.staticdataapi.get_runes)
