# This Python file uses the following encoding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from beem.amount import Amount
from beem.comment import Comment
from beem.utils import resolve_authorperm, construct_authorperm
from datetime import datetime, timedelta
from diff_match_patch import diff_match_patch
from engine.account_storage import AccountsDB
from engine.post_storage import PostsTrx
from engine.post_metadata_storage import PostMetadataStorage
import json
import time
import traceback

import logging

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())


class CommentProcessorForEngine(object):
    """ Processor for handling comment operations for engine comments.
    """

    def __init__(self, db, hived, token_metadata):
        self.db = db
        self.hived = hived
        self.postTrx = PostsTrx(db)
        self.postMetadataStorage = PostMetadataStorage(db)
        self.accountsStorage = AccountsDB(db)
        self.token_metadata = token_metadata

    def process(self, ops):
        """ Main process method.
        """
        token_config = self.token_metadata["config"]
        timestamp = ops["timestamp"].replace(tzinfo=None)

        posts_list = []
        post_metadata_list = []

        comment_start_time = time.time()

        post_author = ops["author"]
        authorperm = construct_authorperm(ops)
        parent_json_metadata = None
        parent_posts = None

        main_post = ops['parent_permlink'] == "" or ops['parent_author'] == ""

        json_metadata = {}
        posts = self.postTrx.get_post(authorperm)

        decline_payout = False
        
        if "title" in ops:
            title = ops["title"]
        else:
            title = None
        children = 0
        try:
            json_metadata = json.loads(ops["json_metadata"])
            if isinstance(json_metadata, str):
                json_metadata = json.loads(json_metadata)
        except:
            print("Metadata error for %s" % authorperm)
            json_metadata = {}

        if not isinstance(json_metadata, dict):
            print("bad nondict json_metadata")
            json_metadata = {}

        tags_set = set()
        tags = ""
        if main_post and ops["parent_permlink"] != "" and "," not in ops["parent_permlink"]:
            # hived repurposes this for category, and it may overlap with tags
            tags_set.add(ops["parent_permlink"])
            tags = ops["parent_permlink"]
        if "tags" in json_metadata and isinstance(json_metadata["tags"], list):
            for t in json_metadata["tags"]:
                if not isinstance(t, str):
                    continue
                if t not in tags_set:
                    tags_set.add(t)
                    if t is not None and len(tags) == 0:
                        tags = t
                    elif t is not None and len(tags) > 0:
                        tags += "," + t

        parent_authorperm = None
        if not main_post:
            parent_authorperm = f"{construct_authorperm(ops['parent_author'], ops['parent_permlink'])}"
            parent_posts = self.postTrx.get_post(parent_authorperm)
            
        app = None

        if posts is not None and len(posts) > 0:
            if posts[0]["decline_payout"] is not None:
                decline_payout = posts[0]["decline_payout"]

            if posts[0]["app"]:
                app = posts[0]["app"]

            if "title" in ops:
                title = ops["title"]
            else:
                title = posts[0]["title"]

            old_post_metadata = self.postMetadataStorage.get(authorperm)

            if "body" in ops:
                try:
                    dmp = diff_match_patch()
                    patch = dmp.patch_fromText(ops["body"])
                    if old_post_metadata and patch is not None and len(patch):
                        new_body, _ = dmp.patch_apply(patch, old_post_metadata["body"])
                    elif patch is not None and len(patch) and not old_post_metadata:
                        print(f"Edit on post not in db, fetching {authorperm}")
                        c = None
                        cnt = 0
                        while c is None and cnt < 5:
                            cnt += 1
                            try:
                                if cnt <= 2:
                                    c = Comment(authorperm, api="bridge", steem_instance=self.hived)
                                else:
                                    c = Comment(authorperm, api="condenser", steem_instance=self.hived)
                            except Exception as e:
                                print(f"Attempt {cnt}: Could not fetch comment")
                                traceback.print_exc()
                                c = None
                        if c is not None:
                            new_body = c["body"]
                        else:
                            new_body = ops["body"]
                    else:
                        new_body = ops["body"]
                except Exception as ex:
                    new_body = ops["body"]

            desc = new_body[:300]
            if posts[0]["children"] is not None:
                children = posts[0]["children"]

            for post in posts:
                token = post["token"]
                posts_list.append({"authorperm": authorperm, "token": token, "title": title[:256], "desc": desc, "tags": tags[:256], "parent_author": ops["parent_author"], "parent_permlink": ops["parent_permlink"], "main_post": main_post, "children": children})

            if main_post:
                self.accountsStorage.upsert({"name": post_author, "symbol": token, "last_root_post": timestamp})
            else:
                self.accountsStorage.upsert({"name": post_author, "symbol": token, "last_post": timestamp})
            if parent_posts is not None:
                for parent_post in parent_posts:
                    children = parent_post["children"]
                    if children is not None:
                        children += 1
                    else:
                        children = 1
                    posts_list.append({"authorperm": parent_post["authorperm"], "token": parent_post["token"],
                                       "children": children})
            post_metadata = {"authorperm": authorperm, "body": new_body, "json_metadata": json.dumps(json_metadata), "parent_authorperm": parent_authorperm, "title": title, "tags": tags}
            if ops['parent_author'] and ops['parent_permlink']:
                parent_post_metadata = self.postMetadataStorage.get(parent_authorperm)
                if parent_post_metadata:
                    children = parent_post_metadata['children']
                    if children is not None:
                        children += 1
                    else:
                        children = 1
                    self.postMetadataStorage.upsert({"authorperm": parent_authorperm, "children": children})
                    if parent_post_metadata["depth"] is not None:
                        post_metadata["depth"] = parent_post_metadata["depth"] + 1
                    if parent_post_metadata["url"] is not None:
                        post_metadata["url"] = parent_post_metadata["url"]
            else:
                post_metadata["depth"] = 0
                post_metadata["url"] = f"/{ops['parent_permlink']}/{authorperm}"

            self.postMetadataStorage.upsert(post_metadata)

        if len(posts_list) > 0:
            self.postTrx.add_batch(posts_list)

        print("Adding comment/post (engine) took %.2f s" % (time.time() - comment_start_time))
