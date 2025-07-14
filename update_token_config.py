#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import dataset
import requests

from engine.token_config_storage import TokenConfigDB
from engine.utils import initialize_config

if __name__ == "__main__":
    config_file = "config.json"
    config_data = initialize_config(config_file)

    db = dataset.connect(config_data["databaseConnector"], ensure_schema=False)
    tokenConfigStorage = TokenConfigDB(db)

    url = "https://smt-api.enginerpc.com/config"
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for bad status codes
    new_configs = response.json()

    for config in new_configs:
        tokenConfigStorage.upsert(config)

    print("Token configuration updated successfully from API!")
