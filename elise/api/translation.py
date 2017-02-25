# Common
COMMON_PLATFORMID = {"BR1": 1,
                     "EUN1": 2,
                     "EUW1": 3,
                     "JP1": 4,
                     "KR": 5,
                     "LA1": 6,
                     "LA2": 7,
                     "NA1": 8,
                     "OC1": 9,
                     "TR1": 10,
                     "RU": 11,
                     "PBE1": 99}

COMMON_ENDPOINT_TO_REGION = {"BR1": "BR",
                             "EUN1": "EUNE",
                             "EUW1": "EUW",
                             "JP1": "JP",
                             "KR": "KR",
                             "LA1": "LAN",
                             "LA2": "LAS",
                             "NA1": "NA",
                             "OC1": "OCE",
                             "TR1": "TR",
                             "RU": "RU",
                             "PBE1": "PBE"}

COMMON_REGION_TO_ENDPOINT = {v: k for k, v in COMMON_ENDPOINT_TO_REGION.items()}

# MatchReference
MATCHREFERENCE_PLATFORMID = COMMON_PLATFORMID

MATCHREFERENCE_QUEUE = {}

MATCHREFERENCE_SEASON = {}

def get_platform_id(region):
    return COMMON_REGION_TO_ENDPOINT[region.upper()]