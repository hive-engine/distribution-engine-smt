#!/usr/bin/python
# -*- coding: utf-8 -*-
import logging
import logging.config
import time
from datetime import datetime, timezone

import dataset
from nectar import Hive
from nectar.block import Blocks
from nectar.blockchain import Blockchain
from nectar.utils import construct_authorperm
from nectarengine.api import Api

from engine.config_storage import ConfigurationDB
from engine.follow_storage import FollowsDB
from engine.post_storage import PostsTrx
from engine.reblog_storage import ReblogsDB
from engine.token_config_storage import TokenConfigDB
from engine.utils import initialize_config, initialize_token_metadata, setup_logging
from processors.comment_processor_for_engine import CommentProcessorForEngine
from processors.custom_json_follow_processor import FollowProcessor
from processors.custom_json_processor import extract_json_data
from processors.custom_json_reblog_processor import ReblogProcessor
from processors.custom_json_set_tribe_settings import SetTribeSettingsProcessor

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logging.basicConfig()


class HiveStreamProcessor:
    def __init__(
        self, db, hived, token_metadata, confStorage, postTrx, reblogsStorage, followsDb
    ):
        self.db = db
        self.hived = hived
        self.token_metadata = token_metadata
        self.confStorage = confStorage
        self.postTrx = postTrx
        self.reblogsStorage = reblogsStorage
        self.followsDb = followsDb

        # Correctly instantiate Blockchain object
        self.blockchain = Blockchain(
            mode="head", max_block_wait_repetition=27, steem_instance=hived
        )

        self.comment_processor_for_engine = CommentProcessorForEngine(
            db, hived, token_metadata
        )
        self.reblog_processor = ReblogProcessor(db, token_metadata)
        self.follow_processor = FollowProcessor(db, token_metadata)
        self.set_tribe_settings_processor = SetTribeSettingsProcessor(
            db, token_metadata
        )

        self.last_streamed_block = 0
        self.last_streamed_timestamp = None
        self.last_engine_streamed_timestamp = None
        self.block_processing_time = time.time()
        self.last_block_print = 0

    def process_op(self, ops):
        engine_conf = self.confStorage.get_engine()
        if engine_conf and engine_conf.get("last_engine_streamed_timestamp"):
            self.last_engine_streamed_timestamp = engine_conf[
                "last_engine_streamed_timestamp"
            ].replace(tzinfo=timezone.utc)

        current_op_block_num = ops["block_num"]

        if current_op_block_num - self.last_streamed_block > 1:
            print(
                f"Skip block last block {self.last_streamed_block} - now {current_op_block_num}"
            )
        elif current_op_block_num - self.last_streamed_block == 1:
            print(f"Current block {current_op_block_num}")

        delay_sec = int((datetime.now(timezone.utc) - ops["timestamp"]).total_seconds())

        if delay_sec < 15:
            print(f"Blocks too recent {delay_sec} ago, waiting.")
            return False  # Indicate that processing should pause

        if (
            not self.last_engine_streamed_timestamp
            or ops["timestamp"] >= self.last_engine_streamed_timestamp
        ):
            print(
                f"Waiting for engine refblock to catch up to {self.last_engine_streamed_timestamp}"
            )
            return False  # Indicate that processing should pause

        delay_string = (
            f"(+ {delay_sec} s): "
            if delay_sec < 60
            else f"(+ {delay_sec / 60:.1f} min)"
        )

        if current_op_block_num > self.last_streamed_block:
            if len(self.token_metadata["config"]) > 0:
                print(
                    f"{delay_string}: Block processing took {time.time() - self.block_processing_time:.2f} s"
                )
            self.block_processing_time = time.time()

            self.confStorage.upsert(
                {
                    "last_streamed_block": self.last_streamed_block,
                    "last_streamed_timestamp": self.last_streamed_timestamp,
                }
            )
            self.db.commit()
            self.db.begin()

        self.last_streamed_block = current_op_block_num
        self.last_streamed_timestamp = ops["timestamp"]

        if current_op_block_num - self.last_block_print > 20:
            self.last_block_print = current_op_block_num
            print(f"{delay_string}: {current_op_block_num} - {ops['timestamp']}")
            # Removed 'blocks left' as stop_block is dynamic in continuous streaming

        if ops["type"] == "custom_json":
            custom_json_start_time = time.time()
            json_data = extract_json_data(ops)
            if (
                json_data
                and ops["id"] == "follow"
                and isinstance(json_data, list)
                and len(json_data) == 2
                and json_data[0] == "reblog"
                and isinstance(json_data[1], dict)
            ):
                self.reblog_processor.process(ops, json_data)
            elif (
                json_data
                and ops["id"] == "reblog"
                and isinstance(json_data, list)
                and len(json_data) == 2
                and json_data[0] == "reblog"
                and isinstance(json_data[1], dict)
            ):
                self.reblog_processor.process(ops, json_data)
            elif json_data and ops["id"] == "follow":
                self.follow_processor.process(ops, json_data)
                print(f"follow op took {time.time() - custom_json_start_time:.2f} s")
            elif json_data and ops["id"] == "scot_set_tribe_settings":
                self.set_tribe_settings_processor.process(ops, json_data)
        elif ops["type"] == "delete_comment":
            try:
                authorperm = construct_authorperm(ops["author"], ops["permlink"])
                self.postTrx.delete_posts(authorperm)
            except Exception:
                print(f"Could not process {authorperm}")
        elif ops["type"] == "comment":
            self.comment_processor_for_engine.process(ops)
        return True

    def run(self):
        # Use self.blockchain here
        current_block_num = self.blockchain.get_current_block_num()
        conf_setup = self.confStorage.get()
        if conf_setup is None:
            self.confStorage.upsert(
                {
                    "last_streamed_block": 0,
                    "last_streamed_timestamp": datetime(
                        1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc
                    ),
                }
            )
            self.last_streamed_block = current_block_num
            self.last_streamed_timestamp = datetime(
                1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc
            )
        else:
            self.last_streamed_block = conf_setup["last_streamed_block"]
            self.last_streamed_timestamp = (
                conf_setup["last_streamed_timestamp"].replace(tzinfo=timezone.utc)
                if conf_setup["last_streamed_timestamp"]
                else datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            )

        engine_conf = self.confStorage.get_engine()
        if engine_conf and engine_conf.get("last_engine_streamed_timestamp"):
            self.last_engine_streamed_timestamp = engine_conf[
                "last_engine_streamed_timestamp"
            ].replace(tzinfo=timezone.utc)

        start_block = (
            self.last_streamed_block + 1
            if self.last_streamed_block != 0
            else self.blockchain.get_current_block_num()
        )
        self.last_block_print = start_block

        print(f"Starting stream from block {start_block}")

        # Changed to a single pass, then exit
        current_head_block = self.blockchain.get_current_block_num()
        if start_block > current_head_block:
            print("Caught up. Exiting...")
            return  # Exit the run method

        if ENABLE_BULK_BLOCKS:
            print(
                f"Starting batch processing from {start_block} to {current_head_block}"
            )
            current_batch_start = start_block
            while current_batch_start <= current_head_block:
                current_batch_end = min(
                    current_batch_start + BATCH_SIZE - 1, current_head_block
                )
                print(f"Processing batch {current_batch_start} - {current_batch_end}")
                blocks_generator = Blocks(
                    starting_block_num=current_batch_start,
                    end_block=current_batch_end,
                    only_ops=True,
                    ops=["comment", "custom_json", "delete_comment"],
                    blockchain_instance=self.hived,
                )
                batch_completed = True
                for block in blocks_generator:
                    print(f"Processing block {block.block_num}")
                    for op in block.operations:
                        op_dict = op["value"]
                        op_dict["type"] = op["type"].replace("_operation", "")
                        op_dict["block_num"] = block.block_num
                        op_dict["timestamp"] = block["timestamp"].replace(
                            tzinfo=timezone.utc
                        )  # Ensure timezone-aware
                        if not self.process_op(op_dict):
                            batch_completed = False
                            break
                    if not batch_completed:
                        break
                if not batch_completed:
                    return  # Exit if process_op returned False
                current_batch_start = current_batch_end + 1
        else:
            print(
                f"Starting stream processing from {start_block} to {current_head_block}"
            )
            # Use self.blockchain here
            for ops in self.blockchain.stream(
                start=start_block,
                stop=current_head_block,  # Stream up to the current head block
                opNames=["comment", "custom_json", "delete_comment"],
                max_batch_size=None,  # Use default streaming behavior
                threading=False,
                thread_num=1,
            ):
                ops["timestamp"] = ops["timestamp"].replace(
                    tzinfo=timezone.utc
                )  # Ensure timezone-aware
                if not self.process_op(ops):
                    break  # Exit loop if process_op returned False

        # Final update of last streamed block and timestamp before exiting
        self.confStorage.upsert(
            {
                "last_streamed_block": self.last_streamed_block,
                "last_streamed_timestamp": self.last_streamed_timestamp,
            }
        )
        self.db.commit()
        print("Stream processing completed. Exiting.")


if __name__ == "__main__":
    setup_logging("logger.json")

    config_file = "config.json"
    config_data = initialize_config(config_file)

    databaseConnector = config_data["databaseConnector"]
    engine_api = Api(url=config_data["engine_api"])
    engine_id = config_data["engine_id"]

    ENABLE_BULK_BLOCKS = bool(config_data.get("enable_hive_bulk_blocks", False))
    BATCH_SIZE = 1000  # Define BATCH_SIZE here

    db = dataset.connect(databaseConnector, ensure_schema=False)

    postTrx = PostsTrx(db)
    confStorage = ConfigurationDB(db)
    tokenConfigStorage = TokenConfigDB(db)
    reblogsStorage = ReblogsDB(db)
    followsDb = FollowsDB(db)

    node_list = ["https://api.syncad.com", "https://api.hive.blog"]
    hived = Hive(node=node_list, num_retries=5, call_num_retries=3, timeout=15)
    print(f"using node {hived.rpc.url}")

    token_config = tokenConfigStorage.get_all()
    token_metadata = initialize_token_metadata(token_config, engine_api)

    processor = HiveStreamProcessor(
        db, hived, token_metadata, confStorage, postTrx, reblogsStorage, followsDb
    )
    processor.run()
