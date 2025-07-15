#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
import json
from datetime import datetime, timezone
from decimal import Decimal

import dataset
import sqltap.wsgi
from flask import (
    Flask,
    jsonify,
    request,
)
from flask_caching import Cache
from flask_compress import Compress
from flask_cors import CORS
from nectar import Hive
from nectar.account import Account
from nectar.comment import Comment
from nectar.utils import (
    construct_authorperm,
    formatTimeString,
    resolve_authorperm,
)
from nectarengine.api import Api
from nectarengine.tokenobject import Token

from engine.account_history_storage import AccountHistoryTrx
from engine.account_storage import AccountsDB
from engine.config_storage import ConfigurationDB
from engine.follow_storage import FollowsDB
from engine.post_metadata_storage import PostMetadataStorage
from engine.post_storage import PostsTrx
from engine.reblog_storage import ReblogsDB
from engine.token_config_storage import TokenConfigDB
from engine.vote_storage import VotesTrx

config_file = "config.json"
with open(config_file) as json_data_file:
    config_data = json.load(json_data_file)

app = Flask(__name__)
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 1
app.config["CACHE_TYPE"] = "filesystem"
app.config["CACHE_DIR"] = config_data["apiCacheDir"]
app.config["CACHE_DEFAULT_TIMEOUT"] = 300
app.config.from_object(__name__)

app.wsgi_app = sqltap.wsgi.SQLTapMiddleware(app.wsgi_app)

CORS(app, supports_credentials=True)
Compress(app)

cache = Cache(app)

databaseConnector = config_data["databaseConnector"]

engine_api = Api(url=config_data["engine_api"])

node_list = ["https://api.deathwing.me", "https://api.hive.blog"]
hived = Hive(
    node=node_list,
    num_retries=5,
    call_num_retries=3,
    timeout=15,
    nobroadcast=True,
    bundle=True,
)


@app.route("/")
def main():
    return ""


@app.route("/state", methods=["GET"])
def state():
    db = dataset.connect(databaseConnector, ensure_schema=False)
    confStorage = ConfigurationDB(db)
    try:
        hived_conf = confStorage.get()
        engine_conf = confStorage.get_engine()
        # Ensure stored timestamp is timezone-aware (assume UTC if naive)
        hived_last_ts = hived_conf["last_streamed_timestamp"]
        if hived_last_ts.tzinfo is None:
            hived_last_ts = hived_last_ts.replace(tzinfo=timezone.utc)
        time_delay_seconds = (
            datetime.now(timezone.utc) - hived_last_ts
        ).total_seconds()
        engine_last_ts = engine_conf["last_engine_streamed_timestamp"]
        if engine_last_ts.tzinfo is None:
            engine_last_ts = engine_last_ts.replace(tzinfo=timezone.utc)
        engine_time_delay_seconds = (
            datetime.now(timezone.utc) - engine_last_ts
        ).total_seconds()
        data = {
            "last_streamed_block": hived_conf["last_streamed_block"],
            "last_streamed_timestamp": formatTimeString(
                hived_conf["last_streamed_timestamp"]
            ),
            "time_delay_seconds": time_delay_seconds,
            "engine_time_delay_seconds": engine_time_delay_seconds,
        }
        return jsonify(data)
    finally:
        db.executable.close()
        db = None


@cache.cached(timeout=60, query_string=True)
@app.route("/info", methods=["GET"])
def token():
    """
    Fetch reward pool info for token
    """
    token = request.args.get("token", None)
    if token:
        token = token.upper()

    db = dataset.connect(databaseConnector, ensure_schema=False)
    tokenConfigStorage = TokenConfigDB(db)
    try:
        if token:
            token_config = tokenConfigStorage.get(token)
            token_data_object = {}
            # fetch from contract
            reward_pool_id = token_config["reward_pool_id"]
            reward_pool = engine_api.find_one(
                "comments", "rewardPools", {"_id": int(reward_pool_id)}
            )
            if isinstance(reward_pool, list) and len(reward_pool) > 0:
                token_data_object["pending_rshares"] = Decimal(
                    reward_pool[0]["pendingClaims"]
                )
                token_data_object["reward_pool"] = Decimal(reward_pool[0]["rewardPool"])

            tokenApi = Token(symbol=token, api=engine_api)
            if tokenApi:
                token_data_object["precision"] = tokenApi["precision"]
                token_data_object["issuer"] = tokenApi["issuer"]
            return jsonify(token_data_object)
        else:
            token_config = tokenConfigStorage.get_all()
            token_data = {}
            for token in token_config:
                reward_pool_id = token_config[token]["reward_pool_id"]
                reward_pool = engine_api.find_one(
                    "comments", "rewardPools", {"_id": int(reward_pool_id)}
                )
                token_data_object = {"token": token}
                if isinstance(reward_pool, list) and len(reward_pool) > 0:
                    token_data_object["pending_rshares"] = Decimal(
                        reward_pool[0]["pendingClaims"]
                    )
                    token_data_object["reward_pool"] = Decimal(
                        reward_pool[0]["rewardPool"]
                    )
                tokenApi = Token(symbol=token, api=engine_api)
                if tokenApi:
                    token_data_object["precision"] = tokenApi["precision"]
                    token_data_object["issuer"] = tokenApi["issuer"]
                token_data[token] = token_data_object
            return jsonify(token_data)
    finally:
        db.executable.close()
        db = None


@app.route("/config", methods=["GET"])
def config():
    """
    Fetch token config
    """
    token = request.args.get("token", None)
    if token:
        token = token.upper()

    db = dataset.connect(databaseConnector, ensure_schema=False)
    try:
        tokenConfigStorage = TokenConfigDB(db)

        if token is None:
            token_config_list = tokenConfigStorage.get_all_list()
            return jsonify(token_config_list)
        else:
            token_config = tokenConfigStorage.get(token)
            if token_config:
                return jsonify(token_config)
            return jsonify({})
    finally:
        db.executable.close()
        db = None


@app.route("/get_account_history", methods=["GET"])
def get_account_history():
    """
    get_account_history
    """
    account = request.args.get("account", None)
    if account is None:
        return jsonify({})

    token = request.args.get("token", None)
    if token:
        token = token.upper()
    limit = request.args.get("limit", 100)
    offset = request.args.get("offset", 0)
    hist_type = request.args.get("type", None)

    try:
        limit = int(limit)
    except Exception:
        return jsonify([])
    try:
        offset = int(offset)
    except Exception:
        return jsonify([])

    db = dataset.connect(databaseConnector, ensure_schema=False)
    try:
        accountHistoryTrx = AccountHistoryTrx(db)
        if hist_type is None and token is None:
            history = accountHistoryTrx.get_history(account, limit, offset)
        elif hist_type is None and token is not None:
            history = accountHistoryTrx.get_token_history(token, account, limit, offset)
        elif hist_type is not None and token is None:
            history = accountHistoryTrx.get_history(
                account, limit, offset, hist_type=hist_type
            )
        elif hist_type is not None and token is not None:
            history = accountHistoryTrx.get_token_history(
                token, account, limit, offset, hist_type=hist_type
            )

        ret = []
        for h in history:
            h2 = {}
            h2["type"] = h["type"]
            if h["timestamp"] is not None:
                h2["timestamp"] = formatTimeString(h["timestamp"])
            if h["authorperm"] is not None:
                author, permlink = resolve_authorperm(h["authorperm"])
                h2["author"] = author
                h2["permlink"] = permlink
            h2["account"] = h["account"]
            h2["token"] = h["token"]
            h2["quantity"] = h["quantity"]
            h2["trx"] = h["trx"]
            ret.append(h2)
        return jsonify(ret)
    finally:
        db.executable.close()
        db = None


@app.route("/@<account>", methods=["GET"])
def account(account):
    """
    Add a new rule
    """
    token = request.args.get("token", None)
    if token:
        token = token.upper()
    db = dataset.connect(databaseConnector, ensure_schema=False)
    try:
        accountsStorage = AccountsDB(db)

        acc_data = accountsStorage.get_all_token(account)

        response = {}
        for t in acc_data:
            if token is not None and token != t:
                continue

            acc_data[t]["name"] = account
            response[t] = acc_data[t]
        return jsonify(response)
    finally:
        db.executable.close()
        db = None


@app.route("/@<account>/<permlink>", methods=["GET"])
def authorperm(account, permlink):
    """
    Add a new rule
    """
    token = request.args.get("token", None)
    if token:
        token = token.upper()
    db = dataset.connect(databaseConnector, ensure_schema=False)
    try:
        authorperm = construct_authorperm(account, permlink)
        postTrx = PostsTrx(db)
        votesTrx = VotesTrx(db)
        post_list = postTrx.get_authorperm_posts(authorperm)

        posts = {}
        for post in post_list:
            if token is not None and token != post["token"]:
                continue

            post["cashout_time"] = formatTimeString(post["cashout_time"])
            post["created"] = formatTimeString(post["created"])
            post["last_payout"] = formatTimeString(post["last_payout"])
            post["title"] = post["title"]
            post["vote_rshares"] = Decimal(post["vote_rshares"])

            post["authorperm"] = authorperm
            post["author"] = account

            vote_list = votesTrx.get_token_vote(authorperm, post["token"])
            for vote in vote_list:
                vote["timestamp"] = formatTimeString(vote["timestamp"])
                vote["percent"] = int(vote["percent"])
            post["active_votes"] = vote_list
            posts[post["token"]] = post

        return jsonify(posts)
    finally:
        db.executable.close()
        db = None


@app.route("/get_staked_accounts", methods=["GET"])
@cache.cached(timeout=86400, query_string=True)
def get_staked_accounts():
    """
    Get Staked Accounts via engine API
    """
    token = request.args.get("token", None)
    if token:
        token = token.upper()
    tokenApi = Token(symbol=token, api=engine_api)
    res = []
    offset = 0
    while True:
        holders = tokenApi.get_holder(1000, offset)
        for holder in holders:
            res.append(
                {"name": holder["account"], "staked_tokens": Decimal(holder["stake"])}
            )
        offset = offset + 1000
        if len(holders) < 1000:
            break
    return jsonify(res)


def format_feed_data(
    db, token, posts, start_author, start_permlink, limit, fetch_votes=True
):
    """
    Attach vote data and massage post output.
    """
    votesTrx = VotesTrx(db)

    output_posts = []
    start_found = False

    for post in posts:
        author, permlink = resolve_authorperm(post["authorperm"])

        if start_permlink is not None and not start_found:
            if start_author == author and start_permlink == permlink:
                start_found = True
            else:
                continue
        post["permlink"] = permlink
        post["cashout_time"] = formatTimeString(post["cashout_time"])
        post["created"] = formatTimeString(post["created"])
        post["last_payout"] = formatTimeString(post["last_payout"])
        post["vote_rshares"] = Decimal(post["vote_rshares"])
        if fetch_votes:
            if not fetch_votes:
                vote_list = votesTrx.get_token_vote(post["authorperm"], token)
            else:
                voter_vote = votesTrx.get(post["authorperm"], fetch_votes, token)
                vote_list = [voter_vote] if voter_vote else []

            for vote in vote_list:
                vote["timestamp"] = formatTimeString(vote["timestamp"])
                vote.pop("authorperm", None)
                if vote["timestamp"] > post["cashout_time"]:
                    continue
        else:
            vote_list = []
        post["active_votes"] = vote_list

        if "reblogged_by" in post and isinstance(post["reblogged_by"], str):
            reblogged_by = post["reblogged_by"].split(",")
            if len(reblogged_by) > 0:
                reblogged_by_user = reblogged_by[0]
                post["reblogged_by"] = [reblogged_by_user]
            else:
                del post["reblogged_by"]
        elif "reblogged_by" in post:
            del post["reblogged_by"]

        post["author"] = author
        post["authorperm"] = construct_authorperm(author, post["permlink"])
        post["hive"] = True

        if start_permlink is not None and start_found:
            output_posts.append(post)
        elif start_permlink is None:
            output_posts.append(post)
        if len(output_posts) >= limit:
            return jsonify(output_posts)
    return jsonify(output_posts)


def fetch_and_save(c, token, postTrx, postMetadataStorage):
    """
    Fetch from hived and save post metadata.
    """
    authorperm = f"@{c.author}/{c.permlink}"
    token_post = postTrx.get_token_post(token, authorperm)
    if not token_post:
        # see from comment processor and match behavior for orphaned data that is not in comments contract
        return []
    replies = c.get_replies()
    results = []
    json_metadata = c.json_metadata
    root_post = c.get_parent()
    this_result = {
        "authorperm": authorperm,
        "body": c.body,
        "json_metadata": json.dumps(json_metadata) if json_metadata else None,
        "tags": ",".join(json_metadata["tags"])
        if json_metadata and "tags" in json_metadata
        else None,
        "parent_authorperm": f"@{c.parent_author}/{c.parent_permlink}"
        if c.parent_author
        else None,
        "children": len(replies),
        "url": f"/{root_post.category}/{root_post.authorperm}",
        "depth": c.depth,
    }
    # print(f"postMetadataStorage.upsert({this_result})")
    postMetadataStorage.upsert(this_result)
    # print(postMetadataStorage.get(this_result["authorperm"]))
    this_result.update(token_post)
    results.append(this_result)
    for reply in replies:
        reply_result = fetch_and_save(reply, token, postTrx, postMetadataStorage)
        results.extend(reply_result)
    return results


@app.route("/get_thread", methods=["GET"])
def get_thread():
    """
    Fetch posts and comments
    """
    token = request.args.get("token", None)
    if token:
        token = token.upper()
    author = request.args.get("author", None)
    permlink = request.args.get("permlink", None)
    refresh = request.args.get("refresh", False)

    if token is None:
        return jsonify([])
    if author is None:
        return jsonify([])
    if permlink is None:
        return jsonify([])

    db = dataset.connect(databaseConnector, ensure_schema=False)
    try:
        postTrx = PostsTrx(db)
        postMetadataStorage = PostMetadataStorage(db)
        posts = list(postTrx.get_thread_discussions(token, author, permlink))
        if (
            refresh
            or (len(posts) == 0)
            or ("url" not in posts[0] or posts[0]["url"] is None)
        ):
            c = Comment(f"{author}/{permlink}", blockchain_instance=hived)
            posts = fetch_and_save(c, token, postTrx, postMetadataStorage)
        return format_feed_data(db, token, posts, None, None, 1000, True)
    finally:
        db.executable.close()
        db = None


@app.route("/get_feed", methods=["GET"])
def get_feed():
    """
    Fetch user's feed
    """
    token = request.args.get("token", None)
    if token:
        token = token.upper()
    account = request.args.get("tag", None)
    limit = request.args.get("limit", 20)
    start_author = request.args.get("start_author", None)
    start_permlink = request.args.get("start_permlink", None)
    include_reblogs = request.args.get("include_reblogs", True)
    fetch_votes = not request.args.get("no_votes", False)
    fetch_votes = request.args.get("voter", fetch_votes)

    try:
        limit = int(limit)
    except Exception:
        return jsonify([])
    if token is None:
        return jsonify([])
    if account is None:
        return jsonify([])
    if start_author is not None and start_permlink is None:
        return jsonify([])
    if start_author is None and start_permlink is not None:
        return jsonify([])

    db = dataset.connect(databaseConnector, ensure_schema=False)
    try:
        postTrx = PostsTrx(db)
        reblogsDb = ReblogsDB(db)

        refresh_follows(db, account)

        last_timestamp = None
        if start_author is not None and start_permlink is not None:
            authorperm = construct_authorperm(start_author, start_permlink)

            post = postTrx.get_token_post(token, authorperm)
            if post is not None:
                last_timestamp = post["created"]
                if include_reblogs:
                    reblog_time = reblogsDb.get_earliest_authorperm_reblog_timestamp(
                        account, authorperm, use_follows=True
                    )
                    if reblog_time is not None and reblog_time > last_timestamp:
                        last_timestamp = reblog_time

        created_posts = postTrx.get_feed_discussions(
            token,
            [account],
            last_timestamp=last_timestamp,
            include_reblogs=include_reblogs,
        )
        return format_feed_data(
            db, token, created_posts, start_author, start_permlink, limit, fetch_votes
        )
    finally:
        db.executable.close()
        db = None


@app.route("/get_discussions_by_created", methods=["GET"])
def get_discussions_by_created():
    token = request.args.get("token", None)
    if token:
        token = token.upper()
    limit = request.args.get("limit", 20)
    tag = request.args.get("tag", None)
    start_author = request.args.get("start_author", None)
    start_permlink = request.args.get("start_permlink", None)
    fetch_votes = not request.args.get("no_votes", False)
    fetch_votes = request.args.get("voter", fetch_votes)
    try:
        limit = int(limit)
    except Exception:
        return jsonify([])
    if token is None:
        return jsonify([])
    if start_author is not None and start_permlink is None:
        return jsonify([])
    if start_author is None and start_permlink is not None:
        return jsonify([])
    db = dataset.connect(databaseConnector, ensure_schema=False)
    try:
        postTrx = PostsTrx(db)

        last_timestamp = None
        if start_author is not None and start_permlink is not None:
            authorperm = construct_authorperm(start_author, start_permlink)
            post = postTrx.get_token_post(token, authorperm)
            if post is not None:
                last_timestamp = post["created"]

        created_posts = postTrx.get_discussions_by_created(
            token, tag=tag, limit=limit, last_timestamp=last_timestamp
        )
        return format_feed_data(
            db, token, created_posts, start_author, start_permlink, limit, fetch_votes
        )
    finally:
        db.executable.close()
        db = None


def get_discussions_by_score(request, score_key, main_post=True):
    token = request.args.get("token", None)
    if token:
        token = token.upper()
    limit = request.args.get("limit", 20)
    tag = request.args.get("tag", None)
    start_author = request.args.get("start_author", None)
    start_permlink = request.args.get("start_permlink", None)
    fetch_votes = not request.args.get("no_votes", False)
    fetch_votes = request.args.get("voter", fetch_votes)
    try:
        limit = int(limit)
    except Exception:
        return jsonify([])
    if token is None:
        return jsonify([])
    if start_author is not None and start_permlink is None:
        return jsonify([])
    if start_author is None and start_permlink is not None:
        return jsonify([])

    db = dataset.connect(databaseConnector, ensure_schema=False)
    try:
        postTrx = PostsTrx(db)

        last_authorperm = None
        if start_author is not None and start_permlink is not None:
            last_authorperm = construct_authorperm(start_author, start_permlink)

        created_posts = postTrx.get_discussions_by_score(
            score_key,
            token,
            tag=tag,
            limit=limit,
            last_authorperm=last_authorperm,
            main_post=main_post,
        )
        return format_feed_data(
            db, token, created_posts, start_author, start_permlink, limit, fetch_votes
        )
    finally:
        db.executable.close()
        db = None


@app.route("/get_discussions_by_trending", methods=["GET"])
def get_discussions_by_trending():
    return get_discussions_by_score(request, "score_trend")


@app.route("/get_discussions_by_promoted", methods=["GET"])
def get_discussions_by_promoted():
    return get_discussions_by_score(request, "promoted")


@app.route("/get_discussions_by_hot", methods=["GET"])
def get_discussions_by_hot():
    return get_discussions_by_score(request, "score_hot")


@app.route("/get_discussions_by_payout", methods=["GET"])
def get_discussions_by_payout():
    return get_discussions_by_score(request, "vote_rshares")


@app.route("/get_comment_discussions_by_payout", methods=["GET"])
def get_comment_discussions_by_payout():
    return get_discussions_by_score(request, "vote_rshares", main_post=False)


@app.route("/get_discussions_by_blog", methods=["GET"])
def get_discussions_by_blog():
    """
    Add a new rule
    """
    token = request.args.get("token", None)
    if token:
        token = token.upper()
    limit = request.args.get("limit", 20)
    account = request.args.get("tag", None)
    include_reblogs = request.args.get("include_reblogs", False)
    start_author = request.args.get("start_author", None)
    start_permlink = request.args.get("start_permlink", None)
    fetch_votes = not request.args.get("no_votes", False)
    fetch_votes = request.args.get("voter", fetch_votes)

    try:
        limit = int(limit)
    except Exception:
        return jsonify([])
    if token is None:
        return jsonify([])
    if account is None:
        return jsonify([])
    if start_author is not None and start_permlink is None:
        return jsonify([])
    if start_author is None and start_permlink is not None:
        return jsonify([])

    db = dataset.connect(databaseConnector, ensure_schema=False)
    try:
        postTrx = PostsTrx(db)
        reblogsDb = ReblogsDB(db)

        last_timestamp = None
        if start_author is not None and start_permlink is not None:
            authorperm = construct_authorperm(start_author, start_permlink)
            if account == start_author:
                post = postTrx.get_token_post(token, authorperm)
                if post is not None:
                    last_timestamp = post["created"]
            else:
                reblog_time = reblogsDb.get_earliest_authorperm_reblog_timestamp(
                    account, authorperm
                )
                if reblog_time is not None:
                    last_timestamp = reblog_time

        created_posts = postTrx.get_discussions_by_blog(
            token,
            [account],
            include_reblogs=include_reblogs,
            last_timestamp=last_timestamp,
        )
        return format_feed_data(
            db, token, created_posts, start_author, start_permlink, limit, fetch_votes
        )
    finally:
        db.executable.close()
        db = None


@app.route("/get_discussions_by_comments", methods=["GET"])
def get_discussions_by_comments():
    """
    Get comments.
    """
    token = request.args.get("token", None)
    if token:
        token = token.upper()
    limit = request.args.get("limit", 20)
    account = request.args.get("tag", None)
    start_author = request.args.get("start_author", None)
    start_permlink = request.args.get("start_permlink", None)
    fetch_votes = not request.args.get("no_votes", False)
    fetch_votes = request.args.get("voter", fetch_votes)

    try:
        limit = int(limit)
    except Exception:
        return jsonify([])
    if token is None:
        return jsonify([])
    if account is None:
        return jsonify([])
    if start_author is not None and start_permlink is None:
        return jsonify([])
    if start_author is None and start_permlink is not None:
        return jsonify([])

    db = dataset.connect(databaseConnector, ensure_schema=False)
    try:
        postTrx = PostsTrx(db)

        last_timestamp = None
        if start_author is not None and start_permlink is not None:
            authorperm = construct_authorperm(start_author, start_permlink)
            post = postTrx.get_token_post(token, authorperm)
            if post is not None:
                last_timestamp = post["created"]

        comment_posts = postTrx.get_discussions_by_comments(
            token, [account], last_timestamp=last_timestamp
        )
        return format_feed_data(
            db, token, comment_posts, start_author, start_permlink, limit, fetch_votes
        )
    finally:
        db.executable.close()
        db = None


@app.route("/get_discussions_by_replies", methods=["GET"])
def get_discussions_by_replies():
    """
    Get replies.
    """
    token = request.args.get("token", None)
    if token:
        token = token.upper()
    limit = request.args.get("limit", 20)
    account = request.args.get("tag", None)
    start_author = request.args.get("start_author", None)
    start_permlink = request.args.get("start_permlink", None)
    fetch_votes = not request.args.get("no_votes", False)
    fetch_votes = request.args.get("voter", fetch_votes)

    try:
        limit = int(limit)
    except Exception:
        return jsonify([])
    if token is None:
        return jsonify([])
    if account is None:
        return jsonify([])
    if start_author is not None and start_permlink is None:
        return jsonify([])
    if start_author is None and start_permlink is not None:
        return jsonify([])

    db = dataset.connect(databaseConnector, ensure_schema=False)
    try:
        postTrx = PostsTrx(db)

        last_timestamp = None
        if start_author is not None and start_permlink is not None:
            authorperm = construct_authorperm(start_author, start_permlink)
            post = postTrx.get_token_post(token, authorperm)
            if post is not None:
                last_timestamp = post["created"]

        reply_posts = postTrx.get_discussions_by_replies(
            token, [account], last_timestamp=last_timestamp
        )
        return format_feed_data(
            db, token, reply_posts, start_author, start_permlink, limit, fetch_votes
        )
    finally:
        db.executable.close()
        db = None


@app.route("/get_trending_tags", methods=["GET"])
@cache.cached(timeout=86400, query_string=True)
def get_trending_tags():
    """
    Get trending tags.
    """
    token = request.args.get("token", None)
    if token:
        token = token.upper()
    limit = request.args.get("limit", 40)

    try:
        limit = int(limit)
    except Exception:
        return jsonify([])
    if token is None:
        return jsonify([])

    db = dataset.connect(databaseConnector, ensure_schema=False)
    try:
        postTrx = PostsTrx(db)

        tags = postTrx.get_trending_tags(token)
        return jsonify(list(tags))
    finally:
        db.executable.close()
        db = None


def refresh_follows(db, account):
    accountsStorage = AccountsDB(db)
    followsDb = FollowsDB(db)
    tokenConfigStorage = TokenConfigDB(db)
    followRefreshTime = accountsStorage.get_follow_refresh_time(account)

    if followRefreshTime is None:
        try:
            acc = Account(account, steem_instance=hived)
        except Exception:
            return
        following = acc.get_following()

        followsDb.refresh_follows(account, following)

        all_tokens = tokenConfigStorage.get_all_list()
        if len(all_tokens) > 0:
            accountsStorage.upsert(
                {
                    "name": account,
                    "symbol": all_tokens[0]["token"],
                    "last_follow_refresh_time": datetime.now(),
                }
            )


@app.route("/get_following", methods=["GET"])
def get_following():
    """
    Get following.
    """
    limit = request.args.get("limit", 1000)
    follower = request.args.get("follower", None)
    following = request.args.get("following", None)
    # For pagination, optional
    start = request.args.get("start", None)
    # Either "ignore" (muted), or "blog" (follow)
    status = request.args.get("status", None)
    hive = request.args.get("hive", False)

    try:
        limit = int(limit)
    except Exception:
        return jsonify([])

    db = dataset.connect(databaseConnector, ensure_schema=False)
    try:
        followsStorage = FollowsDB(db)

        result = [
            {"follower": x["follower"], "following": x["following"]}
            for x in followsStorage.get_following(
                follower, following, status, start=start, limit=limit, hive=hive
            )
        ]
        return jsonify(result)
    finally:
        db.executable.close()
        db = None


@app.route("/get_follow_count", methods=["GET"])
def get_follow_count():
    """
    Get follow count.
    """
    account = request.args.get("account", None)

    db = dataset.connect(databaseConnector, ensure_schema=False)
    try:
        refresh_follows(db, account)
        followsStorage = FollowsDB(db)

        return jsonify(followsStorage.get_follow_count(account))
    finally:
        db.executable.close()
        db = None


if __name__ == "__main__":
    app.run()
