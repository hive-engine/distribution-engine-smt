# This Python file uses the following encoding: utf-8
from __future__ import absolute_import, division, print_function, unicode_literals

import logging

from nectar.utils import construct_authorperm

from processors.custom_json_processor import CustomJsonProcessor, extract_user

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())


class ReblogProcessor(CustomJsonProcessor):
    """Processor for reblog operations."""

    def __init__(self, db, token_metadata):
        super().__init__(db, token_metadata)

    def process(self, ops, json_data):
        """Main process method."""
        token_config = self.token_metadata["config"]
        timestamp = ops["timestamp"].replace(tzinfo=None)

        user = extract_user(ops, json_data)
        if user is None:
            return

        # ["reblog",{"account":"xx","author":"yy","permlink":"zz", "delete":"delete"}]
        if "account" not in json_data[1] or user != json_data[1]["account"]:
            return
        if "author" not in json_data[1] or "permlink" not in json_data[1]:
            return

        authorperm = construct_authorperm(
            json_data[1]["author"], json_data[1]["permlink"]
        )
        posts = self.postTrx.get_post(authorperm)
        if len(posts) > 0 and posts[0]["parent_author"] == "":
            if "delete" in json_data[1] and json_data[1]["delete"] == "delete":
                self.reblogsStorage.delete(user, authorperm)
            else:
                self.reblogsStorage.upsert(
                    {"account": user, "authorperm": authorperm, "timestamp": timestamp}
                )
