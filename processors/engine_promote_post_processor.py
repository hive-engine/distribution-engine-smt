# This Python file uses the following encoding: utf-8
from __future__ import absolute_import, division, print_function, unicode_literals

import json
import logging
import re
import traceback
from decimal import Decimal


from engine.utils import _score
from processors.custom_json_processor import (
    CustomJsonProcessor,
    check_engine_op,
)

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())


class PromotePostProcessor(CustomJsonProcessor):
    """Processor for post promotion operations."""

    def __init__(self, db, token_metadata):
        super().__init__(db, token_metadata)

    def process(self, op, contractPayload):
        """Main process method."""
        token_config = self.token_metadata["config"]
        print(json.dumps(contractPayload))

        if not check_engine_op(op):
            return
        if "symbol" not in contractPayload:
            print("No symbol field in contractPayload")
            print(json.dumps(contractPayload))
            return
        if "quantity" not in contractPayload:
            print("No quantity field in contractPayload")
            print(json.dumps(contractPayload))
            return

        if isinstance(contractPayload["quantity"], (float, int)):
            quantity = contractPayload["quantity"]
        else:
            try:
                quantity = float(contractPayload["quantity"])
            except:
                print("%s is not a valid amount" % contractPayload["quantity"])
                return

        transfer_token = contractPayload["symbol"]

        user = op["sender"]
        if user is None:
            return

        promotion_success = False
        memo = contractPayload["memo"]
        print("promotion detected %s" % str(memo))
        try:
            if memo[0] == "'" or memo[0] == '"':
                memo = memo[1:-1]
            at_sign_position = re.search(r"h?@", memo).start()
            if at_sign_position < 0:
                print("Memo must include @ sign")
                return
            else:
                authorperm = memo[at_sign_position:]

                print(
                    "Transfer to null with memo %s and token %s detected"
                    % (authorperm, transfer_token)
                )
                posts = self.postTrx.get_post(authorperm)
                promoted = 0
                _timestamp = 0
                if len(posts) > 0:
                    for post in posts:
                        token = post["token"]
                        if token == transfer_token:
                            promotion_success = True
                            promoted = Decimal(post["promoted"])
                            _timestamp = post["created"].timestamp()
        except Exception:
            traceback.print_exc()

        if promotion_success:
            promoted_amount = Decimal(quantity)
            promoted = promoted + promoted_amount

            sc_promoted = _score(promoted, _timestamp, 480000)
            self.postTrx.update(
                {
                    "authorperm": authorperm,
                    "token": transfer_token,
                    "promoted": promoted,
                    "score_promoted": sc_promoted,
                }
            )
            print(
                "%s was promoted with %.2f %s" % (authorperm, quantity, transfer_token)
            )
