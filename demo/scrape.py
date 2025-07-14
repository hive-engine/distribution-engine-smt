import json
import time
from typing import Any, Dict

from flask import current_app
from nectar import Hive
from nectar.block import Blocks
from nectar.blockchain import Blockchain
from nectar.nodelist import NodeList

from app.utils.block_storage import ensure_indexes, get_last_block, process_operations

# Configuration
BATCH_SIZE = 1000  # Number of blocks to process at once
SLEEP_TIME = 0.5  # Time to sleep between batches

nodelist = NodeList()
weights = {"block": 1, "history": 0, "apicall": 0, "config": 0}
nodelist.update_nodes(weights)
# Manually updated list of nodes
nodes = nodelist.get_hive_nodes()
# Connect to Hive node
hive = Hive(node=nodes)
genisys = 41818753


def latest() -> int:
    """Get the current head block on the blockchain."""
    return Blockchain(blockchain_instance=hive).get_current_block().block_num


def safe_str(value: Any, default: str = "") -> str:
    """Safely convert value to string."""
    try:
        if value is None:
            return default
        return str(value).strip()
    except Exception:
        return default


def safe_get(obj: Dict, key: str, default: Any = None) -> Any:
    """Safely get a value from a dictionary."""
    try:
        return obj.get(key, default)
    except Exception:
        return default


def process_blocks(app):
    """Main function to process blocks with app context."""
    with app.app_context():
        blocks_collection = current_app.mongo.db.blocks
        ensure_indexes(blocks_collection)

        try:
            current = latest()
            last = get_last_block(blocks_collection) or genisys
            variance = current - last
            current_app.logger.info(
                f"Starting block processing: current={current}, last={last}, variance={variance}"
            )

            # Calculate the number of rounds needed
            rounds = (variance // BATCH_SIZE) + 1

            for _r in range(rounds):
                start_time = time.time()
                blocks = Blocks(
                    last + 1,
                    BATCH_SIZE,
                    only_ops=True,
                    ops=["comment_operation"],
                    blockchain_instance=hive,
                )
                operations = []
                num_coms = 0

                for block in blocks:
                    last = block.block_num
                    for operation in block.operations:
                        if (
                            operation["type"] == "comment_operation"
                            and operation["value"]["parent_author"] == ""
                        ):
                            try:
                                metadata = dict(
                                    json.loads(
                                        operation["value"]["json_metadata"].replace(
                                            "\\", ""
                                        )
                                    )
                                )
                            except Exception:
                                current_app.logger.debug(
                                    f"Failed to parse metadata for post by {operation['value']['author']}"
                                )
                                metadata = {}
                                continue

                            # Basic post data
                            data = {
                                "block_num": block.block_num,
                                "permlink": operation["value"]["permlink"],
                                "tags": safe_get(metadata, "tags"),
                                "author": operation["value"]["author"],
                                "timestamp": block["timestamp"],
                                "title": operation["value"]["title"],
                                "app": safe_get(metadata, "app"),
                            }

                            # Extract images from metadata if available
                            if (
                                "image" in metadata
                                and isinstance(metadata["image"], list)
                                and metadata["image"]
                            ):
                                data["images"] = metadata["image"]

                            # Try to get community, category, and additional data using Comment API
                            try:
                                author = operation["value"]["author"]
                                permlink = operation["value"]["permlink"]
                                authorperm = f"{author}/{permlink}"

                                # Create a Comment object to get additional data
                                # We use a short timeout to avoid slowing down the scraping process too much
                                comment = None
                                try:
                                    from nectar.comment import Comment

                                    comment = Comment(
                                        authorperm, blockchain_instance=hive, timeout=2
                                    )
                                except Exception as comment_err:
                                    current_app.logger.debug(
                                        f"Failed to create Comment object for {authorperm}: {str(comment_err)}"
                                    )

                                if comment:
                                    # Extract category
                                    if (
                                        hasattr(comment, "category")
                                        and comment.category
                                    ):
                                        data["category"] = comment.category

                                    # Extract community ID (if category is a community ID)
                                    if (
                                        hasattr(comment, "category")
                                        and comment.category
                                        and comment.category.startswith("hive-")
                                    ):
                                        data["community"] = comment.category

                                    # Extract community title
                                    if (
                                        hasattr(comment, "community_title")
                                        and comment.community_title
                                    ):
                                        data["community_title"] = (
                                            comment.community_title
                                        )

                                    # Check URL for additional community info if needed
                                    if (
                                        "community" not in data
                                        and hasattr(comment, "url")
                                        and comment.url
                                    ):
                                        parts = comment.url.strip("/").split("/")
                                        if (
                                            len(parts) >= 3
                                            and parts[0]
                                            and not parts[0].startswith("@")
                                        ):
                                            data["community"] = parts[0]

                                    # Get raw data to extract additional fields
                                    raw_data = comment.json()
                                    if (
                                        "community_title" not in data
                                        and "community_title" in raw_data
                                    ):
                                        data["community_title"] = raw_data[
                                            "community_title"
                                        ]

                                    # Double-check for images if we didn't get them from metadata
                                    if "images" not in data and hasattr(
                                        comment, "json_metadata"
                                    ):
                                        json_md = comment.json_metadata
                                        if (
                                            isinstance(json_md, dict)
                                            and "image" in json_md
                                        ):
                                            images = json_md["image"]
                                            if isinstance(images, list) and images:
                                                data["images"] = images
                            except Exception as e:
                                current_app.logger.debug(
                                    f"Error fetching additional data for {operation['value']['author']}/{operation['value']['permlink']}: {str(e)}"
                                )
                            operations.append(data)
                            num_coms += 1

                            # Process in smaller batches if we've accumulated enough operations
                            if len(operations) >= 500:
                                process_operations(blocks_collection, operations)
                                current_app.logger.debug(
                                    f"Processed batch of {len(operations)} operations"
                                )
                                operations = []

                # Process any remaining operations
                if operations:
                    process_operations(blocks_collection, operations)
                    current_app.logger.debug(
                        f"Processed final batch of {len(operations)} operations"
                    )

                end_time = time.time()
                elapsed_time = end_time - start_time
                percentage_complete = ((last - genisys) / (current - genisys)) * 100
                current_app.logger.info(
                    f"Block {last}: {round(percentage_complete, 4)}% complete. "
                    f"Processed {len(blocks)} blocks with {num_coms} posts in {round(elapsed_time, 4)} seconds."
                )
                time.sleep(SLEEP_TIME)

        except KeyboardInterrupt:
            current_app.logger.info("Gracefully shutting down block processing...")
        except Exception as e:
            current_app.logger.error(f"Error processing blocks: {str(e)}")
