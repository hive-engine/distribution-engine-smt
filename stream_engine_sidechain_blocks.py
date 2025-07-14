#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import logging
import logging.config
import os
import time
import traceback

import dataset
from dotenv import load_dotenv
from nectar.utils import parse_time
from nectarengine.api import Api

from engine.config_storage import ConfigurationDB
from engine.token_config_storage import TokenConfigDB
from engine.utils import initialize_config, initialize_token_metadata, setup_logging
from processors.engine_comments_contract_processor import CommentsContractProcessor
from processors.engine_promote_post_processor import PromotePostProcessor
load_dotenv()



logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig()

# Flag to enable bulk sidechain block fetching (up to 1000 at a time)
ENABLE_BULK_BLOCKS = os.getenv("ENGINE_BULK_BLOCKS", "false").lower() in {"1", "true", "yes"}

if __name__ == "__main__":
    setup_logging("logger.json")

    config_file = "config.json"
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

    def process_engine_block(block_dict):
        """Process a single sidechain block returned by the engine API."""
        timestamp = parse_time(block_dict["timestamp"])

        db.begin()
        if not block_dict["transactions"]:
            print("No transactions in block.")
        else:
            for op in block_dict["transactions"]:
                contract_action = op["action"]
                contractPayload = op["payload"]
                try:
                    contractPayload = json.loads(contractPayload)

                    if op["contract"] == "comments":
                        comments_processor.process(op, contractPayload, timestamp)
                        continue
                    elif op["contract"] == "tokens" and contract_action == "transfer":
                        if (
                            "memo" not in contractPayload
                            or contractPayload["memo"] is None
                        ):
                            print("No memo field in contractPayload")
                            continue
                        memo = contractPayload["memo"]
                        if not isinstance(memo, str) or len(memo) < 3:
                            return
                        if "symbol" not in contractPayload:
                            return
                        transfer_token = contractPayload["symbol"]
                        if "to" not in contractPayload:
                            return
                        if not isinstance(transfer_token, str):
                            return
                        if transfer_token not in token_metadata["config"]:
                            return
                        transfer_token_config = token_metadata["config"][transfer_token]
                        if transfer_token_config is not None and memo.find("@") > -1:
                            if (
                                contractPayload["to"]
                                == transfer_token_config["promoted_post_account"]
                            ):
                                promote_post_processor.process(op, contractPayload)
                except Exception:
                    traceback.print_exc()
                    raise "Error"

        confStorage.upsert_engine(
            {
                "last_engine_streamed_block": block_dict["blockNumber"],
                "last_engine_streamed_timestamp": timestamp,
            }
        )
        db.commit()

    if ENABLE_BULK_BLOCKS:
        print("Bulk block fetching ENABLED. Fetching sidechain blocks in chunks of 1000 …")
        CHUNK_SIZE = 1000
        current = start_block
        while current < stop_block:
            count = min(CHUNK_SIZE, stop_block - current)
            try:
                block_range = engine_api.get_block_range_info(current, count)
            except Exception:
                # Fall back to single-block fetch on error
                traceback.print_exc()
                block_range = []
            for block_dict in block_range:
                process_engine_block(block_dict)
            current += count
    else:
        for current_block_num in range(start_block, stop_block):
            current_block = engine_api.get_block_info(current_block_num)
            print(f"Processing engine block {current_block_num}")
            process_engine_block(current_block)
