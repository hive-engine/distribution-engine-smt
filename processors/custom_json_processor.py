# This Python file uses the following encoding: utf-8
from __future__ import absolute_import, division, print_function, unicode_literals

import json
import logging
import traceback


from engine.account_history_storage import AccountHistoryTrx
from engine.account_storage import AccountsDB
from engine.follow_storage import FollowsDB
from engine.post_storage import PostsTrx
from engine.reblog_storage import ReblogsDB
from engine.token_config_storage import TokenConfigDB
from engine.vote_storage import VotesTrx

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())


def extract_json_data(ops):
    """Extract JSON data."""
    json_data = None
    try:
        json_data = json.loads(ops["json"])
        if isinstance(json_data, str):
            json_data = json.loads(json_data)
    except:
        traceback.print_exc()
        print("Skip json: %s" % str(ops["json"]))
    return json_data


def extract_user(ops, json_data):
    """Extract user from JSON data."""
    if json_data is None:
        return None
    if "required_posting_auths" not in ops or "required_auths" not in ops:
        return None
    if ops["required_posting_auths"] is None or ops["required_auths"] is None:
        return None
    if len(ops["required_posting_auths"]) > 0:
        return ops["required_posting_auths"][0]
    elif len(ops["required_auths"]) > 0:
        return ops["required_auths"][0]
    else:
        print("Cannot parse transaction, as user could not be determined!")
    return None


def check_engine_op(op):
    """Check Engine API transaction."""
    if op is not None and "logs" in op:
        logs = json.loads(op["logs"])
        if isinstance(logs, str):
            logs = json.loads(logs)
        if "errors" not in logs:
            return True
        elif logs["errors"] == ["contract doesn't exist"]:
            # Ignore witness contract not existing, happens for ENG / BEE staking
            return True
        else:
            print(op["logs"])
            print("Op has errors.")
            return False
    print("Op has no logs.")
    return True


class CustomJsonProcessor(object):
    """Base processor for handling custom json operations."""

    def __init__(self, db, token_metadata):
        self.db = db
        self.postTrx = PostsTrx(db)
        self.voteTrx = VotesTrx(db)
        self.accountsStorage = AccountsDB(db)
        self.reblogsStorage = ReblogsDB(db)
        self.followsDb = FollowsDB(db)
        self.tokenConfigStorage = TokenConfigDB(db)
        self.accountHistoryTrx = AccountHistoryTrx(db)
        self.token_metadata = token_metadata
