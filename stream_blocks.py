#!/usr/bin/python
# -*- coding: utf-8 -*-
from datetime import datetime, timezone
import logging
import logging.config
import time

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


def process_op(ops):
    global \
        current_block_num, \
        last_streamed_timestamp, \
        last_streamed_block, \
        block_processing_time, \
        last_block_print, \
        start_block, \
        stop_block, \
        token_config, \
        confStorage, \
        db

    current_op_block_num = ops["block_num"]

    if current_op_block_num - current_block_num > 1:
        print(
            "Skip block last block %d - now %d"
            % (current_block_num, current_op_block_num)
        )
    elif current_op_block_num - current_block_num == 1:
        print("Current block %d" % current_op_block_num)

    current_block_num = current_op_block_num

    delay_min = (datetime.now(timezone.utc) - ops["timestamp"]).total_seconds() / 60
    delay_sec = int((datetime.now(timezone.utc) - ops["timestamp"]).total_seconds())

    if delay_sec < 15:
        print(f"Blocks too recent {delay_sec} ago, waiting.")
        confStorage.upsert(
            {
                "last_streamed_block": current_op_block_num,
                "last_streamed_timestamp": ops["timestamp"],
            }
        )
        db.commit()
        return False
    if (
        not last_engine_streamed_timestamp
        or ops["timestamp"] >= last_engine_streamed_timestamp
    ):
        print(
            f"Waiting for engine refblock to catch up to {last_engine_streamed_timestamp}"
        )
        confStorage.upsert(
            {
                "last_streamed_block": current_op_block_num,
                "last_streamed_timestamp": ops["timestamp"],
            }
        )
        db.commit()
        return False

    if delay_min < 1:
        delay_string = "(+ %d s): " % (delay_sec)
    else:
        delay_string = "(+ %.1f min)" % (delay_min)

    if current_op_block_num > last_streamed_block:
        if len(token_config) > 0:
            print(
                "%s: Block processing took %.2f s"
                % (delay_string, time.time() - block_processing_time)
            )
        block_processing_time = time.time()

        confStorage.upsert(
            {
                "last_streamed_block": current_op_block_num,
                "last_streamed_timestamp": ops["timestamp"],
            }
        )

        # this is end of a block. end tx here
        if start_block < last_streamed_block + 1:
            db.commit()
        db.begin()

    last_streamed_block = current_op_block_num
    last_streamed_timestamp = ops["timestamp"]

    if ops["block_num"] - last_block_print > 20:
        last_block_print = ops["block_num"]
        print("%s: %d - %s" % (delay_string, ops["block_num"], str(ops["timestamp"])))
        print("blocks left %d" % (stop_block - ops["block_num"]))

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
            reblog_processor.process(ops, json_data)
        elif (
            json_data
            and ops["id"] == "reblog"
            and isinstance(json_data, list)
            and len(json_data) == 2
            and json_data[0] == "reblog"
            and isinstance(json_data[1], dict)
        ):
            reblog_processor.process(ops, json_data)
        elif json_data and ops["id"] == "follow":
            follow_processor.process(ops, json_data)
            print("follow op took %.2f s" % (time.time() - custom_json_start_time))
        elif json_data and ops["id"] == "scot_set_tribe_settings":
            set_tribe_settings_processor.process(ops, json_data)
    elif ops["type"] == "delete_comment":
        try:
            authorperm = construct_authorperm(ops["author"], ops["permlink"])
            postTrx.delete_posts(authorperm)
        except Exception:
            print("Could not process %s" % authorperm)
    elif ops["type"] == "comment":
        comment_processor_for_engine.process(ops)
    return True


if __name__ == "__main__":
    setup_logging("logger.json")

    config_file = "config.json"
    config_data = initialize_config(config_file)

    databaseConnector = config_data["databaseConnector"]
    engine_api = Api(url=config_data["engine_api"])
    engine_id = config_data["engine_id"]

    # Read configuration flag for bulk block processing (defaults to False if not present)
    ENABLE_BULK_BLOCKS = bool(config_data.get("enable_bulk_blocks", False))

    start_prep_time = time.time()

    # ensure_schema False, require all indexes be created up front to not waste space
    # (e.g. vote primary key lookup doesn't need a redundant index)
    db = dataset.connect(databaseConnector, ensure_schema=False)
    # Create keyStorage

    postTrx = PostsTrx(db)
    confStorage = ConfigurationDB(db)
    tokenConfigStorage = TokenConfigDB(db)
    reblogsStorage = ReblogsDB(db)
    followsDb = FollowsDB(db)

    max_batch_size = None
    threading = False

    node_list = ["https://api.syncad.com", "https://api.hive.blog"]
    hived = Hive(node=node_list, num_retries=5, call_num_retries=3, timeout=15)
    print("using node %s" % hived.rpc.url)
    b = Blockchain(mode="head", max_block_wait_repetition=27, steem_instance=hived)

    try:
        current_block_num = b.get_current_block_num()
        conf_setup = confStorage.get()
        if conf_setup is None:
            confStorage.upsert({"last_streamed_block": 0})
            last_streamed_block = current_block_num
            last_streamed_timestamp = None
        else:
            last_streamed_block = conf_setup["last_streamed_block"]
            last_streamed_timestamp = conf_setup["last_streamed_timestamp"]
        last_engine_streamed_timestamp = None
        engine_conf = confStorage.get_engine()
        if engine_conf:
            last_engine_streamed_timestamp = engine_conf[
                "last_engine_streamed_timestamp"
            ].replace(tzinfo=timezone.utc)
        if last_streamed_block == 0:
            start_block = current_block_num
            confStorage.upsert({"last_streamed_block": start_block})
        else:
            start_block = last_streamed_block + 1
        stop_block = current_block_num
        print("processing blocks %d - %d" % (start_block, stop_block))
        if start_block >= stop_block:
            print("Caught up. Waiting for new blocks...")
            time.sleep(3)
        last_block_print = start_block
        token_config = tokenConfigStorage.get_all()
        token_metadata = initialize_token_metadata(token_config, engine_api)
        # Processors
        comment_processor_for_engine = CommentProcessorForEngine(
            db, hived, token_metadata
        )
        reblog_processor = ReblogProcessor(db, token_metadata)
        follow_processor = FollowProcessor(db, token_metadata)
        set_tribe_settings_processor = SetTribeSettingsProcessor(db, token_metadata)
        block_processing_time = time.time()
        # Batch processing logic controlled by ENV flag
        BATCH_SIZE = 1000
        if ENABLE_BULK_BLOCKS and stop_block - start_block > 10:
            print("Starting batch processing...")
            while True:
                # Update current block to check for new blocks
                current_block_num = b.get_current_block_num()
                start_block = last_streamed_block + 1
                stop_block = current_block_num

                if start_block >= stop_block:
                    print("Caught up. Waiting for new blocks...")
                    time.sleep(3)
                    continue

                print(f"Processing blocks {start_block} - {stop_block}")
                current_batch_start = start_block

                while current_batch_start <= stop_block:
                    current_batch_end = min(
                        current_batch_start + BATCH_SIZE - 1, stop_block
                    )
                    print(
                        f"Processing batch {current_batch_start} - {current_batch_end}"
                    )
                    blocks_generator = Blocks(
                        starting_block_num=current_batch_start,
                        end_block=current_batch_end,
                        only_ops=True,
                        ops=["comment", "custom_json", "delete_comment"],
                        blockchain_instance=hived,
                    )
                    batch_completed = True
                    for block in blocks_generator:
                        for op in block.operations:
                            op_dict = op["value"]
                            op_dict["type"] = op["type"].replace("_operation", "")
                            op_dict["block_num"] = block.block_num
                            op_dict["timestamp"] = block["timestamp"]
                            if not process_op(op_dict):
                                batch_completed = False
                                break
                        if not batch_completed:
                            break
                    if not batch_completed:
                        break
                    current_batch_start = current_batch_end + 1
        else:
            print("Starting stream processing...")
            for ops in b.stream(
                start=start_block,
                stop=stop_block,
                opNames=["comment", "custom_json", "delete_comment"],
                max_batch_size=max_batch_size,
                threading=threading,
                thread_num=8,
            ):
                if not process_op(ops):
                    break
            if stop_block >= start_block:
                confStorage.upsert(
                    {
                        "last_streamed_block": last_streamed_block,
                        "last_streamed_timestamp": last_streamed_timestamp,
                    }
                )
                db.commit()
        print("stream posts script run %.2f s" % (time.time() - start_prep_time))
    except KeyboardInterrupt:
        print("Exiting...")
