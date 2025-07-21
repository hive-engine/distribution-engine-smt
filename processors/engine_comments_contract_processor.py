# This Python file uses the following encoding: utf-8

import json
import logging
import traceback
from datetime import timedelta
from decimal import Decimal

from nectarengine.tokenobject import Token

from engine.utils import _score
from processors.custom_json_processor import CustomJsonProcessor

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())


class CommentsContractProcessor(CustomJsonProcessor):
    """Processor for comments contract operations."""

    def __init__(self, db, api, token_metadata):
        super().__init__(db, token_metadata)
        self.api = api

    def process(self, op, contractPayload, timestamp):
        """Main process method."""
        token_config = self.token_metadata["config"]
        token_objects = self.token_metadata["objects"]
        token_config_by_id = self.token_metadata["config_by_id"]

        logs = json.loads(op["logs"])
        if op["action"] == "setMute" and "errors" not in logs:
            account = contractPayload["account"]
            reward_pool_id = contractPayload["rewardPoolId"]
            reward_pool = token_config_by_id[reward_pool_id]
            account_obj = self.accountsStorage.get(account, reward_pool["token"])
            if not account_obj:
                account_obj = {"name": account, "symbol": reward_pool["token"]}
            account_obj["muted"] = contractPayload["mute"]
            self.accountsStorage.upsert(account_obj)
        elif op["action"] == "setPostMute" and "errors" not in logs:
            authorperm = contractPayload["authorperm"]
            reward_pool_id = contractPayload["rewardPoolId"]
            reward_pool = token_config_by_id[reward_pool_id]
            post = self.postTrx.get_token_post(reward_pool["token"], authorperm)
            if post:
                post["muted"] = contractPayload["mute"]
                self.postTrx.upsert(post)

        if "events" in logs:
            events = logs["events"]
            paid_out_posts = {}
            for event in events:
                if event["contract"] == "comments":
                    if event["event"] == "newComment":
                        token = event["data"]["symbol"]
                        cashout_window_days = token_config[token]["cashout_window_days"]
                        author = contractPayload["author"]
                        authorperm = f"@{author}/{contractPayload['permlink']}"
                        account_obj = self.accountsStorage.get(author, token)
                        muted = bool(account_obj and account_obj["muted"])
                        self.postTrx.upsert(
                            {
                                "authorperm": authorperm,
                                "author": author,
                                "created": timestamp,
                                "token": token,
                                "cashout_time": timestamp
                                + timedelta(cashout_window_days),
                                "main_post": False,
                                "muted": muted,
                            }
                        )
                    elif event["event"] == "newVote" or event["event"] == "updateVote":
                        token = event["data"]["symbol"]
                        authorperm = f"@{contractPayload['author']}/{contractPayload['permlink']}"
                        voter = contractPayload["voter"]
                        rshares = Decimal(event["data"]["rshares"])
                        old_rshares = Decimal(0)
                        old_vote = self.voteTrx.get(authorperm, voter, token)
                        if old_vote:
                            old_rshares = old_vote["rshares"]
                        self.voteTrx.add_batch(
                            [
                                {
                                    "authorperm": authorperm,
                                    "voter": voter,
                                    "token": token,
                                    "timestamp": timestamp,
                                    "rshares": rshares,
                                    "percent": contractPayload["weight"],
                                }
                            ]
                        )
                        voted_post = self.postTrx.get_token_post(token, authorperm)
                        if voted_post:
                            updated_vote_rshares = (
                                voted_post["vote_rshares"] + rshares - old_rshares
                            )
                            sc_trend = _score(
                                updated_vote_rshares, timestamp.timestamp(), 480000
                            )
                            sc_hot = _score(
                                updated_vote_rshares, timestamp.timestamp(), 10000
                            )
                            self.postTrx.upsert(
                                {
                                    "authorperm": authorperm,
                                    "token": token,
                                    "vote_rshares": updated_vote_rshares,
                                    "score_trend": sc_trend,
                                    "score_hot": sc_hot,
                                }
                            )
                    elif event["event"] == "curationReward":
                        token = event["data"]["symbol"]
                        authorperm = event["data"]["authorperm"]
                        if authorperm not in paid_out_posts:
                            paid_out_posts[authorperm] = {
                                "token": token,
                                "authorperm": authorperm,
                                "last_payout": timestamp,
                                "total_payout_value": 0,
                                "curator_payout_value": 0,
                                "beneficiaries_payout_value": 0,
                            }
                        try:
                            curation_share = Decimal(event["data"]["quantity"])
                            paid_out_posts[authorperm]["curator_payout_value"] += (
                                curation_share
                            )
                            paid_out_posts[authorperm]["total_payout_value"] += (
                                curation_share
                            )
                            if curation_share > 0:
                                self.accountHistoryTrx.add(
                                    {
                                        "account": event["data"]["account"],
                                        "token": token,
                                        "timestamp": timestamp,
                                        "quantity": curation_share,
                                        "type": "curation_reward",
                                        "authorperm": authorperm,
                                        "trx": op["transactionId"],
                                    }
                                )
                        except Exception:
                            traceback.print_exc()

                    elif event["event"] == "beneficiaryReward":
                        token = event["data"]["symbol"]
                        authorperm = event["data"]["authorperm"]
                        if authorperm not in paid_out_posts:
                            paid_out_posts[authorperm] = {
                                "token": token,
                                "authorperm": authorperm,
                                "last_payout": timestamp,
                                "total_payout_value": 0,
                                "curator_payout_value": 0,
                                "beneficiaries_payout_value": 0,
                            }
                        try:
                            beneficiary_share = Decimal(event["data"]["quantity"])
                            paid_out_posts[authorperm][
                                "beneficiaries_payout_value"
                            ] += beneficiary_share
                            paid_out_posts[authorperm]["total_payout_value"] += (
                                beneficiary_share
                            )
                            if beneficiary_share > 0:
                                self.accountHistoryTrx.add(
                                    {
                                        "account": event["data"]["account"],
                                        "token": token,
                                        "timestamp": timestamp,
                                        "quantity": beneficiary_share,
                                        "type": "curation_reward",
                                        "authorperm": authorperm,
                                        "trx": op["transactionId"],
                                    }
                                )
                        except Exception:
                            traceback.print_exc()

                    elif event["event"] == "authorReward":
                        token = event["data"]["symbol"]
                        authorperm = event["data"]["authorperm"]
                        if authorperm not in paid_out_posts:
                            paid_out_posts[authorperm] = {
                                "token": token,
                                "authorperm": authorperm,
                                "last_payout": timestamp,
                                "total_payout_value": 0,
                                "curator_payout_value": 0,
                                "beneficiaries_payout_value": 0,
                            }
                        author_share = Decimal(event["data"]["quantity"])
                        paid_out_posts[authorperm]["total_payout_value"] += author_share
                        paid_out_posts[authorperm]["vote_rshares"] = 0
                        paid_out_posts[authorperm]["score_hot"] = 0
                        paid_out_posts[authorperm]["score_trend"] = 0
                        if author_share > 0:
                            self.accountHistoryTrx.add(
                                {
                                    "account": event["data"]["account"],
                                    "token": token,
                                    "timestamp": timestamp,
                                    "quantity": author_share,
                                    "type": "author_reward",
                                    "authorperm": authorperm,
                                    "trx": op["transactionId"],
                                }
                            )
                    elif (
                        event["event"] == "createRewardPool"
                        or event["event"] == "updateRewardPool"
                    ):
                        token = contractPayload["symbol"]
                        reward_pool_id = (
                            event["data"]["_id"]
                            if "_id" in event["data"]
                            else token_config[token]["reward_pool_id"]
                        )
                        self.tokenConfigStorage.upsert(
                            {
                                "token": token,
                                "author_curve_exponent": contractPayload["config"][
                                    "postRewardCurveParameter"
                                ],
                                "curation_curve_exponent": contractPayload["config"][
                                    "curationRewardCurveParameter"
                                ],
                                "curation_reward_percentage": contractPayload["config"][
                                    "curationRewardPercentage"
                                ],
                                "cashout_window_days": contractPayload["config"][
                                    "cashoutWindowDays"
                                ],
                                "reward_pool_id": reward_pool_id,
                                "reward_per_interval": contractPayload["config"][
                                    "rewardPerInterval"
                                ],
                                "reward_interval_seconds": contractPayload["config"][
                                    "rewardIntervalSeconds"
                                ],
                                "vote_regeneration_days": contractPayload["config"][
                                    "voteRegenerationDays"
                                ],
                                "downvote_regeneration_days": contractPayload["config"][
                                    "downvoteRegenerationDays"
                                ],
                                "staked_reward_percentage": contractPayload["config"][
                                    "stakedRewardPercentage"
                                ],
                                "vote_power_consumption": contractPayload["config"][
                                    "votePowerConsumption"
                                ],
                                "downvote_power_consumption": contractPayload["config"][
                                    "downvotePowerConsumption"
                                ],
                                "tags": ",".join(contractPayload["config"]["tags"]),
                                "issuer": op["sender"],
                                "disable_downvoting": "disableDownvote"
                                in contractPayload["config"]
                                and contractPayload["config"]["disableDownvote"],
                                "ignore_decline_payout": "ignoreDeclinePayout"
                                in contractPayload["config"]
                                and contractPayload["config"]["ignoreDeclinePayout"],
                            }
                        )
                        token_config[token] = self.tokenConfigStorage.get(token)
                        token_config_by_id[reward_pool_id] = token_config[token]
                        token_objects[token] = Token(token, api=self.api)
                    else:
                        print(event)
            for paid_out_post in paid_out_posts.values():
                old_paid_out_post = self.postTrx.get_token_post(
                    paid_out_post["token"], paid_out_post["authorperm"]
                )
                if not old_paid_out_post:
                    self.postTrx.update(
                        {
                            "token": paid_out_post["token"],
                            "authorperm": paid_out_post["authorperm"],
                            "last_payout": paid_out_post["last_payout"],
                            "total_payout_value": paid_out_post["total_payout_value"],
                            "curator_payout_value": paid_out_post[
                                "curator_payout_value"
                            ],
                        }
                    )
                else:
                    if old_paid_out_post["total_payout_value"]:
                        print(
                            f"! updating existing {old_paid_out_post}, {paid_out_post}"
                        )
                    old_paid_out_post["total_payout_value"] += paid_out_post[
                        "total_payout_value"
                    ]
                    old_paid_out_post["curator_payout_value"] += paid_out_post[
                        "curator_payout_value"
                    ]
                    self.postTrx.update(old_paid_out_post)
