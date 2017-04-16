import os
import pathlib
import logging
import time
import argparse
import signal
import sys
import threading

import cassiopeia.baseriotapi

import elise
import elise.type.queuepipe
import elise.thread.dispatch
import elise.thread.request
import elise.thread.flush
import elise.api.translation
import elise.db.discovery


VALID_REGIONS = ("BR", "EUNE", "EUW", "JP", "KR", "LAN", "LAS", "NA", "OCE", "RU", "TR")
VALID_QUEUES = ("RANKED_FLEX_SR", "RANKED_SOLO_5x5", "RANKED_TEAM_3x3", "RANKED_TEAM_5x5", "TEAM_BUILDER_DRAFT_RANKED_5x5", "TEAM_BUILDER_RANKED_SOLO")
VALID_SEASONS = ("PRESEASON3", "SEASON3", "PRESEASON2014", "SEASON2014", "PRESEASON2015", "SEASON2015", "PRESEASON2016", "SEASON2016", "PRESEASON2017", "SEASON2017")


def main(parameters):

    elise.parameters = parameters

    cassiopeia.baseriotapi.set_api_key(parameters["key"])
    cassiopeia.baseriotapi.set_region(parameters["region"])
    cassiopeia.baseriotapi.print_calls(False)

    region = parameters["region"]

    # Initialize folder structure
    folder_data = pathlib.Path('.', 'data')
    folder_region = pathlib.Path(folder_data, region.lower())
    folder_db = pathlib.Path(folder_region, 'database')
    folder_json_match = pathlib.Path(folder_region, 'json-match')
    try:
        folder_data.mkdir(exist_ok=True)
        folder_region.mkdir(exist_ok=True)
        folder_db.mkdir(exist_ok=True)
        folder_json_match.mkdir(exist_ok=True)
    except OSError:
        logging.error("Failed to create folder structure")
        sys.exit(3)

    filename_dbd_match = pathlib.Path(folder_db, "db-discovery-match-" + region.lower() + ".sqlite")
    filename_dbd_matchlist = pathlib.Path(folder_db, "db-discovery-matchlist-" + region.lower() + ".sqlite")
    
    elise.flag_initialized = threading.Event()
    if parameters["cold"]:
        cold_start = True
    else:
        cold_start = False
        elise.flag_initialized.set()

    elise.platformId = elise.api.translation.get_platform_id(region)

    elise.db.discovery.init_discovery_db_match(str(filename_dbd_match))
    elise.db.discovery.init_discovery_db_matchlist(str(filename_dbd_matchlist))
    pipe = elise.type.queuepipe.QueuePipe()

    thread_dispatch_matchlist = elise.thread.dispatch.DispatchMatchlistThread(pipe, str(filename_dbd_matchlist), cold_start=cold_start)
    thread_dispatch_matchlist.start()

    thread_dispatch_match = elise.thread.dispatch.DispatchMatchThread(pipe, str(filename_dbd_match))
    thread_dispatch_match.start()

    thread_flush_match = elise.thread.flush.FlushMatchThread(pipe, folder_json_match)
    thread_flush_match.start()

    thread_request = elise.thread.request.RequestThread(pipe, cold_start=cold_start)
    thread_request.start()

    while thread_dispatch_match.is_alive() and thread_dispatch_matchlist.is_alive() and thread_flush_match.is_alive():
        try:
            if not thread_request.is_alive():
                logging.error("Request thread is dead!!! Restarting...")
                thread_request = elise.thread.request.RequestThread(pipe)
                thread_request.start()
            logging.info("Queues: RM {} / RML {} / FM {} / FML {} / DMF {} / DMLF {} / DMD: {}".format(pipe.request_match.qsize(), pipe.request_matchlist.qsize(), pipe.flush_match.qsize(), pipe.flush_matchlist.qsize(), pipe.dispatch_match_flush.qsize(), pipe.dispatch_matchlist_flush.qsize(), pipe.dispatch_matchlist_discover.qsize()))
            time.sleep(60)
        except KeyboardInterrupt:
            logging.warning("Exiting...")
            thread_request.flag_exit.set()
            elise.api.retry.exit = True
            if thread_request.is_alive():
                thread_request.join()
            thread_flush_match.flag_exit.set()
            if thread_flush_match.is_alive():
                thread_flush_match.join()
            thread_dispatch_matchlist.flag_exit.set()
            if thread_dispatch_matchlist.is_alive():
                thread_dispatch_matchlist.join()
            thread_dispatch_match.flag_exit.set()
            if thread_dispatch_match.is_alive():
                thread_dispatch_match.join()

    logging.info("Program finished")


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="Retrieves full match and summoner info.")
        parser.add_argument("-k", "--key", help="developer or production key provided by Riot", nargs=1)
        parser.add_argument("-r", "--region", help="server region for calls to the API", choices=VALID_REGIONS, required=True, nargs=1, type=str.upper)
        parser.add_argument("-c", "--cold", help="cold start: use challenger / master tiers for seeding", action="store_true", default=False)
        parser.add_argument("-t", "--notimeline", help="don't request timeline data", action="store_true", default=False)
        parser.add_argument("-bt", "--begintime", help="begin time for matchlists", nargs=1, type=int, default=0)
        parser.add_argument("-et", "--endtime", help="end time for matchlists", nargs=1, type=int, default=0)
        parser.add_argument("-nm", "--nummatches", help="number of match references to obtain from matchlist", nargs=1, type=int, default=0)
        parser.add_argument("-bi", "--beginindex", help="end time for matchlists", nargs=1, type=int, default=0)
        parser.add_argument("-ch", "--champions", help="list of integer champion ids for matchlists", nargs="+", type=int, default=None)
        parser.add_argument("-q", "--queues", help="ranked queues to fetch", nargs="+", choices=VALID_QUEUES, type=str.lower, default=None)
        parser.add_argument("-s", "--seasons", help="seasons to fetch", nargs="+", choices=VALID_SEASONS, type=str.lower, default=None)
        parser.add_argument("-v", "--verbose", help="verbose (debug) output", action="store_true", default=False)

    except argparse.ArgumentError as e:
        logging.error("Invalid argument: {0}.".format(e.message))
        sys.exit(2)
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(format='%(asctime)s: %(message)s', level=logging.DEBUG, datefmt='%d/%m/%Y %H:%M:%S')
    else:
        logging.basicConfig(format='%(asctime)s: %(message)s', level=logging.INFO, datefmt='%d/%m/%Y %H:%M:%S')

    parameters = {}

    # Initializes fundamental variables.
    # Initializes API key.
    if args.key is not None:
        logging.info("API key set by command-line argument.")
        parameters["key"] = args.key
    else:
        try:
            parameters["key"] = os.environ["DEV_KEY"]
            logging.info("API key set by environment variable DEV_KEY.")
        except KeyError:
            logging.error("API key was not set. Set key with -k argument or set environment variable DEV_KEY.")
            sys.exit(2)

    # Initializes region.
    if args.region is None:
        logging.error("Region not set. Set region with -r argument.")
        sys.exit(2)
    else:
        logging.info("Region set to {0}.".format(args.region[0].upper()))
        parameters["region"] = args.region[0]

    if args.cold:
        logging.info("Cold-start set.")
        parameters["cold"] = True
    else:
        parameters["cold"] = False

    # Sets SIGINT handler
    #elise.original_sigint = signal.getsignal(signal.SIGINT)
    #signal.signal(signal.SIGINT, exit_gracefully)

    # Calls main().
    main(parameters)



