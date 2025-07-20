#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import logging
import logging.config
import time
import traceback
from datetime import datetime, timezone

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


class EngineStreamProcessor:
    def __init__(self, db, engine_api, token_metadata, confStorage):
        self.db = db
        self.engine_api = engine_api
        self.token_metadata = token_metadata
        self.confStorage = confStorage

        self.promote_post_processor = PromotePostProcessor(db, token_metadata)
        self.comments_processor = CommentsContractProcessor(db, engine_api, token_metadata)

        self.last_engine_streamed_block = 0
        self.last_engine_streamed_timestamp = None
        self.last_block_print = 0

    def process_engine_block(self, block_dict):
        timestamp = parse_time(block_dict["timestamp"]).replace(tzinfo=timezone.utc)

        self.db.begin()
        if not block_dict["transactions"]:
            print("No transactions in block.")
        else:
            for op in block_dict["transactions"]:
                contract_action = op["action"]
                contractPayload = op["payload"]
                try:
                    contractPayload = json.loads(contractPayload)

                    if op["contract"] == "comments":
                        self.comments_processor.process(op, contractPayload, timestamp)
                        continue
                    elif (
                        op["contract"] == "tokens"
                        and contract_action == "transfer"
                    ):
                        if (
                            "memo" not in contractPayload
                            or contractPayload["memo"] is None
                        ):
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
                        if transfer_token not in self.token_metadata["config"]:
                            continue
                        transfer_token_config = self.token_metadata["config"][
                            transfer_token
                        ]
                        if (
                            transfer_token_config is not None
                            and memo.find("@") > -1
                        ):
                            if (
                                contractPayload["to"]
                                == transfer_token_config[
                                    "promoted_post_account"
                                ]
                            ):
                                self.promote_post_processor.process(
                                    op, contractPayload
                                )
                except Exception as e:
                    logger.error(f"Error processing contract action: {e}")
                    traceback.print_exc()

        self.confStorage.upsert_engine(
            {
                "last_engine_streamed_block": block_dict["blockNumber"],
                "last_engine_streamed_timestamp": timestamp,
            }
        )
        self.db.commit()

    def run(self):
        conf_setup = self.confStorage.get_engine()
        if conf_setup is None:
            self.confStorage.upsert_engine({"last_engine_streamed_block": 0, "last_engine_streamed_timestamp": datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)})
            self.last_engine_streamed_block = 0
            self.last_engine_streamed_timestamp = datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        else:
            self.last_engine_streamed_block = conf_setup["last_engine_streamed_block"]
            self.last_engine_streamed_timestamp = conf_setup["last_engine_streamed_timestamp"].replace(tzinfo=timezone.utc) if conf_setup["last_engine_streamed_timestamp"] else datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

        print("stream new engine blocks")

        while True:
            try:
                current_block_info = self.engine_api.get_latest_block_info()
                current_block_num = current_block_info["blockNumber"]

                start_block = self.last_engine_streamed_block + 1
                stop_block = current_block_num + 1

                if start_block >= stop_block:
                    print("Caught up. Waiting for new blocks...")
                    time.sleep(3)
                    self.last_engine_streamed_block = current_block_num
                    continue

                print(f"Processing blocks {start_block} - {stop_block}")

                if ENABLE_BULK_BLOCKS:
                    print("Bulk block fetching ENABLED. Fetching sidechain blocks in chunks of 1000 …")
                    CHUNK_SIZE = 1000
                    current = start_block
                    while current < stop_block:
                        count = min(CHUNK_SIZE, stop_block - current)
                        try:
                            block_range = self.engine_api.get_block_range_info(current, count)
                        except Exception:
                            traceback.print_exc()
                            block_range = []
                        for block_dict in block_range:
                            print(f"Processing engine block {block_dict['blockNumber']}")
                            self.process_engine_block(block_dict)
                            self.last_engine_streamed_block = block_dict["blockNumber"]
                        current += count
                else:
                    for current_block_num_iter in range(start_block, stop_block):
                        current_block = self.engine_api.get_block_info(current_block_num_iter)
                        print(f"Processing engine block {current_block_num_iter}")
                        self.process_engine_block(current_block)
                        self.last_engine_streamed_block = current_block_num_iter

            except KeyboardInterrupt:
                print("Exiting...")
                break
            except Exception as e:
                logger.error(f"An error occurred in the main loop: {e}")
                traceback.print_exc()
                time.sleep(5)


if __name__ == "__main__":
    setup_logging("logger.json")

    config_file = "config.json"
    config_data = initialize_config(config_file)
    databaseConnector = config_data["databaseConnector"]
    engine_api = Api(url=config_data["engine_api"])
    engine_id = config_data["engine_id"]

    ENABLE_BULK_BLOCKS = bool(config_data.get("enable_engine_bulk_blocks", False))

    db = dataset.connect(databaseConnector, ensure_schema=False)

    confStorage = ConfigurationDB(db)
    tokenConfigStorage = TokenConfigDB(db)

    token_config = tokenConfigStorage.get_all()
    token_metadata = initialize_token_metadata(token_config, engine_api)

    processor = EngineStreamProcessor(db, engine_api, token_metadata, confStorage)
    processor.run()