# This Python file uses the following encoding: utf-8
from __future__ import absolute_import, division, print_function, unicode_literals

import logging


from processors.custom_json_processor import CustomJsonProcessor, extract_user

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())


class SetTribeSettingsProcessor(CustomJsonProcessor):
    """Processor for setting tribe settings not in reward pool."""

    def __init__(self, db, token_metadata):
        super().__init__(db, token_metadata)

    def process(self, ops, json_data):
        """Main process method."""
        token_config = self.token_metadata["config"]
        token_objects = self.token_metadata["objects"]
        token_config_by_id = self.token_metadata["config_by_id"]
        timestamp = ops["timestamp"].replace(tzinfo=None)

        user = extract_user(ops, json_data)
        if user is None:
            return

        if "reward_pool_id" not in json_data:
            return
        reward_pool_id = json_data["reward_pool_id"]

        if reward_pool_id not in token_config_by_id:
            return

        reward_pool = self.token_metadata["config_by_id"][reward_pool_id]
        token = reward_pool["token"]
        token_object = token_objects[token]

        if user != token_object["issuer"]:
            print("User not issuer")
            return

        if "promoted_post_account" in json_data:
            reward_pool["promoted_post_account"] = json_data["promoted_post_account"]
        self.tokenConfigStorage.upsert(reward_pool)
        token_config[token] = self.tokenConfigStorage.get(token)
        token_config_by_id[reward_pool_id] = token_config[token]
