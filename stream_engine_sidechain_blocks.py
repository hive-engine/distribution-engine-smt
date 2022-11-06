#!/usr/bin/python
# -*- coding: utf-8 -*-
from beem.utils import formatTimeString, resolve_authorperm, construct_authorperm, addTzInfo, parse_time
from beem.nodelist import NodeList
from beem.comment import Comment
from beem import Hive
from datetime import datetime, timedelta
from beem.instance import set_shared_steem_instance
from beem.blockchain import Blockchain
from beem.block import Block
from beem.account import Account
from beem.amount import Amount
from beem.memo import Memo
import time
import json
import os
import sys
import math
import dataset
import random
import logging
import logging.config
import traceback
from datetime import date, datetime, timedelta
from dateutil.parser import parse
from decimal import Decimal
from engine.config_storage import ConfigurationDB
from engine.token_config_storage import TokenConfigDB
from engine.version import version as engineversion
from engine.utils import setup_logging, int_sqrt, int_pow, _score, convergent_linear, convergent_square_root, initialize_config, initialize_token_metadata
from processors.engine_comments_contract_processor import CommentsContractProcessor
from processors.engine_promote_post_processor import PromotePostProcessor
from steemengine.api import Api
from beem.block import Block
import hashlib
import random
import base36
import dataset
import re

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig()

if __name__ == "__main__":
    setup_logging('logger.json')

    config_file = 'config.json'
    config_data = initialize_config(config_file)
    databaseConnector = config_data["databaseConnector"]
    engine_api = Api(url=config_data["engine_api"])
    engine_id = config_data["engine_id"]

    start_prep_time = time.time()

    # ensure_schema False, require all indexes be created up front to not waste space
    # (e.g. vote primary key lookup doesn't need a redundant index)
    db = dataset.connect(databaseConnector, ensure_schema=False)

    confStorage = ConfigurationDB(db)
    tokenConfigStorage = TokenConfigDB(db)

    token_config = tokenConfigStorage.get_all()
    token_metadata = initialize_token_metadata(token_config, engine_api)

    conf_setup = confStorage.get_engine()
    if conf_setup is None:
        confStorage.upsert_engine({"last_engine_streamed_block": 0})
        last_engine_streamed_block = 0
        last_engine_streamed_timestamp = 0
    else:
        last_engine_streamed_block = conf_setup["last_engine_streamed_block"]
        last_engine_streamed_timestamp = conf_setup["last_engine_streamed_timestamp"]

    print("stream new engine blocks")

    current_block = engine_api.get_latest_block_info()
    current_block_num = current_block["blockNumber"]

    if last_engine_streamed_block == 0:
        start_block = current_block_num
        confStorage.upsert_engine({"last_engine_streamed_block": start_block})
    else:
        start_block = last_engine_streamed_block + 1

    stop_block = current_block_num + 1

    print("start_block %d - %d" % (start_block, stop_block))

    last_block_print = start_block

    last_engine_streamed_timestamp = None

    # Processors
    promote_post_processor = PromotePostProcessor(db, token_metadata)
    comments_processor = CommentsContractProcessor(db, engine_api, token_metadata)

    for current_block_num in range(start_block, stop_block):
        current_block = engine_api.get_block_info(current_block_num)
        print(f"Processing engine block {current_block_num}")
        timestamp = parse_time(current_block["timestamp"]).replace(tzinfo=None)
        last_engine_streamed_timestamp = timestamp
        main_chain_block_num = current_block["refHiveBlockNumber"]

        db.begin()

        if not current_block["transactions"]:
            print(f"No transactions in block.")
        else:
            for op in current_block["transactions"]:
                tx_id = op["transactionId"]

                contract_action = op["action"]
                contractPayload = op["payload"]
                try:
                    contractPayload = json.loads(contractPayload)

                    if op["contract"] == "comments":
                        comments_processor.process(op, contractPayload, timestamp)
                        continue
                    elif op["contract"] == "tokens" and contract_action == "transfer":
                        if "memo" not in contractPayload or contractPayload["memo"] is None:
                            print("No memo field in contractPayload")
                            continue
                        memo = contractPayload["memo"]
                        if not isinstance(memo, str) or len(memo) < 3:
                            continue
                        if "symbol" not in contractPayload:
                            continue
                        transfer_token = contractPayload["symbol"]
                        if "to" not in contractPayload:
                            continue
                        if not isinstance(transfer_token, str):
                            continue
                        if transfer_token not in token_metadata["config"]:
                            continue
                        transfer_token_config = token_metadata["config"][transfer_token]
                        if transfer_token_config is not None and memo.find("@") > -1:
                            if contractPayload["to"] == transfer_token_config["promoted_post_account"]:
                                promote_post_processor.process(op, contractPayload)
                except Exception as e:
                    traceback.print_exc()
                    raise "Error"

        confStorage.upsert_engine({"last_engine_streamed_block": current_block_num, "last_engine_streamed_timestamp": last_engine_streamed_timestamp})
        db.commit()
