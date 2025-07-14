#!/usr/bin/python
import json
import logging
import math
import os

from nectarengine.tokenobject import Token

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig()


def setup_logging(default_path="logging.json", default_level=logging.INFO):
    """Setup logging configuration"""
    path = default_path
    if os.path.exists(path):
        with open(path, "rt") as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def int_sqrt(x):
    return int(math.sqrt(x))


def int_pow(x, y):
    return int(math.pow(x, y))


def convergent_linear(rshares, s):
    return int(((rshares + s) * (rshares + s) - s * s) / (rshares + 4 * s))


def convergent_square_root(rshares, s):
    return int(rshares / int_sqrt(rshares + 2 * s))


def _score(rshares, created_timestamp, timescale=480000):
    """Calculate trending/hot score."""
    mod_score = rshares
    order = math.log10(max((abs(mod_score), 1)))
    sign = 1 if mod_score > 0 else -1
    return sign * order + created_timestamp / timescale


def initialize_config(config_file):
    if not os.path.isfile(config_file):
        raise Exception("config.json is missing!")
    else:
        with open(config_file) as json_data_file:
            config_data = json.load(json_data_file)
        if "engine_api" in config_data:
            print(f"Using {config_data['engine_api']} steemsc node")
        else:
            raise Exception("Please provide engine_api in config.json")
        if "engine_id" not in config_data:
            config_data["engine_id"] = "ssc-mainnet-hive"
    return config_data


def initialize_token_metadata(token_config, engine_api):
    token_objects = {}
    token_config_by_id = {}
    for token in token_config:
        token_objects[token] = Token(token, api=engine_api)
        token_config_by_id[token_config[token]["reward_pool_id"]] = token_config[token]
    return {
        "config": token_config,
        "objects": token_objects,
        "config_by_id": token_config_by_id,
    }
