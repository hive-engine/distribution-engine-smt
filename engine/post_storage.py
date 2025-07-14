# This Python file uses the following encoding: utf-8

import logging
from builtins import object
from datetime import datetime

from dateutil.relativedelta import relativedelta

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

timeformat = "%Y%m%d-%H%M%S"


class PostsTrx(object):
    """This is the trx storage class"""

    __tablename__ = "posts"

    def __init__(self, db):
        self.db = db

    def exists_table(self):
        """Check if the database table exists"""

        if len(self.db.tables) == 0:
            return False
        if self.__tablename__ in self.db.tables:
            return True
        else:
            return False

    def add(self, data):
        """Add a new data set"""
        table = self.db[self.__tablename__]
        table.upsert(data, ["authorperm", "token"])

    def add_batch(self, data):
        """Add a new data set"""
        table = self.db[self.__tablename__]
        # self.db.begin()
        if isinstance(data, list):
            # table.insert_many(data, chunk_size=chunk_size)
            for d in data:
                table.upsert(d, ["authorperm", "token"])
        else:
            for d in data:
                table.upsert(data[d], ["authorperm", "token"])

        # self.db.commit()

    def update_batch(self, data):
        """Add a new data set"""
        table = self.db[self.__tablename__]
        # self.db.begin()
        if isinstance(data, list):
            for d in data:
                table.update(d, ["authorperm", "token"])
        else:
            for d in data:
                table.update(data[d], ["authorperm", "token"])
        # self.db.commit()

    def update(self, data):
        """Change share_age depending on timestamp"""
        table = self.db[self.__tablename__]
        table.update(data, ["authorperm", "token"])

    def upsert(self, data):
        """Change share_age depending on timestamp"""
        table = self.db[self.__tablename__]
        table.upsert(data, ["authorperm", "token"])

    def get_latest_token_post(self, token, author):
        table = self.db[self.__tablename__]
        ret = table.find_one(token=token, author=author, order_by="-created")
        return ret

    def get_latest_block(self):
        table = self.db[self.__tablename__]
        ret = table.find_one(order_by="-created")
        if ret is None:
            return None
        return ret["block"]

    def get_author_posts(self, author):
        table = self.db[self.__tablename__]
        posts = []
        for post in table.find(author=author, order_by="created"):
            posts.append(post)
        return posts

    def get_authorperm_posts(self, authorperm):
        table = self.db[self.__tablename__]
        posts = []
        for post in table.find(authorperm=authorperm, order_by="created"):
            posts.append(post)
        return posts

    def get(self, token):
        table = self.db[self.__tablename__]
        return table.find(token=token, order_by="created")

    def get_limit_by_created(self, token, oldest_created_date):
        table = self.db[self.__tablename__]
        return table.find(
            token=token, created={">=": oldest_created_date}, order_by="created"
        )

    def get_pending_posts(self, token, oldest_created_date):
        table = self.db[self.__tablename__]
        return table.find(
            token=token,
            last_payout=datetime(1970, 1, 1, 0, 0, 0),
            created={">=": oldest_created_date},
            order_by="created",
        )

    def get_oldest_pending_posts(self, token, newest_created_date):
        table = self.db[self.__tablename__]
        return table.find(
            token=token,
            last_payout=datetime(1970, 1, 1, 0, 0, 0),
            created={"<": newest_created_date},
            order_by="created",
        )

    def get_posts(self):
        table = self.db[self.__tablename__]
        posts = {}
        for post in table.find(order_by="created"):
            posts[post["authorperm"]] = post
        return posts

    def get_post(self, authorperm):
        table = self.db[self.__tablename__]
        posts = []
        for post in table.find(authorperm=authorperm):
            posts.append(post)
        return posts

    def get_token_post(self, token, authorperm):
        table = self.db[self.__tablename__]
        return table.find_one(token=token, authorperm=authorperm)

    def get_posts_list(self, start_timestamp):
        table = self.db[self.__tablename__]
        posts = []
        for post in table.find(
            table.table.columns.created > start_timestamp, order_by="created"
        ):
            posts.append(post)
        return posts

    def get_authorperm(self):
        table = self.db[self.__tablename__]
        posts = {}
        for post in table.find(order_by="created"):
            posts[post["authorperm"]] = post["authorperm"]
        return posts

    def get_authorperm_list(self):
        table = self.db[self.__tablename__]
        posts = []
        for post in table.find(order_by="created"):
            posts.append(post["authorperm"])
        return posts

    def get_discussions_by_created(
        self, token, tag=None, limit=100, last_timestamp=None, hive_select=None
    ):
        cutoff = (
            last_timestamp if last_timestamp else datetime.utcnow()
        ) + relativedelta(months=-1)
        tag_clause = ""
        last_timestamp_clause = ""
        hive_select_clause = ""

        if tag is not None:
            tag_clause = "AND STRING_TO_ARRAY(p.tags, ',') @> :tags "
        if last_timestamp is not None:
            last_timestamp_clause = "AND p.created <= :last_timestamp  "
        if hive_select is not None:
            if not hive_select or hive_select == "0":
                hive_select_clause = "AND p.authorperm not like 'h@%' "
            else:
                hive_select_clause = "AND p.authorperm like 'h@%' "

        q = (
            "SELECT p.*, pm.json_metadata FROM posts p LEFT JOIN accounts acc ON p.author = acc.name AND p.token = acc.symbol LEFT JOIN post_metadata pm ON p.authorperm = pm.authorperm WHERE p.muted = 'false' AND (acc is NULL OR acc.muted = 'false') AND p.token = :token AND p.main_post = 'true' AND p.created > :cutoff %s %s %s ORDER BY created DESC LIMIT :limit"
            % (tag_clause, last_timestamp_clause, hive_select_clause)
        )
        return self.db.query(
            q,
            tags=[tag],
            token=token,
            last_timestamp=last_timestamp,
            limit=limit,
            hive_select=hive_select,
            cutoff=cutoff,
        )

    def get_discussions_by_blog(
        self,
        token,
        accounts,
        include_reblogs=False,
        last_timestamp=None,
        limit=100,
        hive_select=None,
    ):
        cutoff = (
            last_timestamp if last_timestamp else datetime.utcnow()
        ) + relativedelta(months=-1)
        # cutoff_clause = "AND p.created > :cutoff"
        cutoff_clause = ""
        timestamp_clause = ""
        reblog_timestamp_clause = ""
        hive_select_clause = ""
        if last_timestamp is not None:
            reblog_timestamp_clause = "AND merged.t <= :last_timestamp "
            timestamp_clause = "AND p.created <= :last_timestamp "
        if hive_select is not None:
            if not hive_select or hive_select == "0":
                hive_select_clause = "AND p.authorperm not like 'h@%' "
            else:
                hive_select_clause = "AND p.authorperm like 'h@%' "

        if include_reblogs:
            return self.db.query(
                f"SELECT index.reblogged_by, p2.*, pm.json_metadata FROM (SELECT authorperm, reblogged_by, MIN(t) t FROM (SELECT r.account reblogged_by, p.authorperm, r.timestamp t FROM posts p INNER JOIN reblogs r ON p.authorperm = r.authorperm LEFT JOIN accounts acc ON p.author = acc.name AND p.token = acc.symbol WHERE p.muted = 'false' AND (acc is NULL OR acc.muted = 'false') AND token = :token AND main_post = 'true' AND r.account != p.author AND r.account in :accounts {cutoff_clause} {hive_select_clause} UNION SELECT NULL reblogged_by, p.authorperm, p.created t FROM posts p WHERE token = :token AND main_post = 'true' AND p.muted = 'false' AND p.author in :accounts {cutoff_clause} {hive_select_clause}) AS merged WHERE TRUE {reblog_timestamp_clause} GROUP BY authorperm, reblogged_by ORDER BY t desc LIMIT :limit) index INNER JOIN posts p2 ON p2.authorperm = index.authorperm AND p2.token = :token LEFT JOIN post_metadata pm ON p2.authorperm = pm.authorperm",
                token=token,
                accounts=tuple(accounts),
                last_timestamp=last_timestamp,
                limit=str(limit),
                cutoff=cutoff,
            )
        else:
            return self.db.query(
                f"SELECT p.*, pm.json_metadata FROM posts p LEFT JOIN accounts acc ON p.author = acc.name AND p.token = acc.symbol LEFT JOIN post_metadata pm ON p.authorperm = pm.authorperm WHERE p.muted = 'false' AND (acc is NULL OR acc.muted = 'false') AND token = :token AND main_post = 'true' AND p.author in :accounts {cutoff_clause} {timestamp_clause} {hive_select_clause} ORDER BY p.created desc LIMIT :limit",
                token=token,
                accounts=tuple(accounts),
                last_timestamp=last_timestamp,
                limit=str(limit),
                cutoff=cutoff,
            )

    def get_discussions_by_comments(
        self, token, accounts, last_timestamp=None, limit=100, hive_select=None
    ):
        cutoff = (
            last_timestamp if last_timestamp else datetime.utcnow()
        ) + relativedelta(months=-1)
        timestamp_clause = ""
        hive_select_clause = ""
        if last_timestamp is not None:
            timestamp_clause = "AND p.created <= :last_timestamp "
        if hive_select is not None:
            if not hive_select or hive_select == "0":
                hive_select_clause = "AND p.authorperm not like 'h@%' "
            else:
                hive_select_clause = "AND p.authorperm like 'h@%' "

        return self.db.query(
            (
                "SELECT p.*, pm.json_metadata FROM posts p LEFT JOIN accounts acc ON p.author = acc.name AND p.token = acc.symbol LEFT JOIN post_metadata pm ON p.authorperm = pm.authorperm WHERE p.muted = 'false' AND (acc is NULL OR acc.muted = 'false') AND token = :token AND main_post = 'false' AND p.author in :accounts AND p.created > :cutoff %s %s ORDER BY p.created desc LIMIT :limit"
                % (timestamp_clause, hive_select_clause)
            ),
            token=token,
            accounts=tuple(accounts),
            last_timestamp=last_timestamp,
            limit=str(limit),
            cutoff=cutoff,
        )

    def get_discussions_by_replies(
        self, token, accounts, last_timestamp=None, limit=100, hive_select=None
    ):
        cutoff = (
            last_timestamp if last_timestamp else datetime.utcnow()
        ) + relativedelta(months=-1)
        timestamp_clause = ""
        hive_select_clause = ""
        if last_timestamp is not None:
            timestamp_clause = "AND p.created <= :last_timestamp "
        if hive_select is not None:
            if not hive_select or hive_select == "0":
                hive_select_clause = "AND p.authorperm not like 'h@%' "
            else:
                hive_select_clause = "AND p.authorperm like 'h@%' "

        return self.db.query(
            (
                "SELECT p.*, pm.json_metadata FROM posts p LEFT JOIN accounts acc ON p.author = acc.name AND p.token = acc.symbol LEFT JOIN post_metadata pm ON p.authorperm = pm.authorperm WHERE p.muted = 'false' AND (acc is NULL OR acc.muted = 'false') AND token = :token AND main_post = 'false' AND p.author NOT IN :accounts AND p.parent_author in :accounts AND p.created > :cutoff %s %s ORDER BY p.created desc LIMIT :limit"
                % (timestamp_clause, hive_select_clause)
            ),
            token=token,
            accounts=tuple(accounts),
            last_timestamp=last_timestamp,
            limit=str(limit),
            cutoff=cutoff,
        )

    def get_thread_discussions(self, token, author, permlink):
        authorperm = f"@{author}/{permlink}"
        return self.db.query(
            "WITH RECURSIVE post_tree AS ( SELECT authorperm, body, json_metadata, 0 __d FROM post_metadata WHERE authorperm = :authorperm UNION SELECT pm.authorperm, pm.body, pm.json_metadata, pt.__d + 1 FROM post_metadata pm INNER JOIN post_tree pt ON pm.parent_authorperm = pt.authorperm WHERE pt.__d <= 8) SELECT pt.__d, p.*, pt.body, pt.json_metadata FROM post_tree pt join posts p ON p.authorperm = pt.authorperm and p.token = :token",
            authorperm=authorperm,
            token=token,
        )

    def get_feed_discussions(
        self,
        token,
        accounts,
        last_timestamp=None,
        limit=100,
        include_reblogs=True,
        hive_select=None,
    ):
        cutoff = (
            last_timestamp if last_timestamp else datetime.utcnow()
        ) + relativedelta(months=-1)
        timestamp_clause = ""
        noreblog_timestamp_clause = ""
        hive_select_clause = ""
        if last_timestamp is not None:
            timestamp_clause = "WHERE merged.t <= :last_timestamp "
            noreblog_timestamp_clause = "AND p.created <= :last_timestamp "
        if hive_select is not None:
            if not hive_select or hive_select == "0":
                hive_select_clause = "AND p.authorperm not like 'h@%' "
            else:
                hive_select_clause = "AND p.authorperm like 'h@%' "

        if include_reblogs:
            return self.db.query(
                (
                    "WITH following_table AS (SELECT following from follows where follower IN :accounts AND state = 1) SELECT index.accounts reblogged_by, p2.*, pm.json_metadata FROM (SELECT authorperm, string_agg(account, ',') accounts, MIN(t) t FROM (SELECT p.authorperm, r.account, r.timestamp t FROM posts p INNER JOIN reblogs r ON p.authorperm = r.authorperm LEFT JOIN accounts acc ON p.author = acc.name AND p.token = acc.symbol WHERE author NOT IN :accounts AND p.muted = 'false' AND (acc is NULL OR acc.muted = 'false') AND token = :token AND main_post = 'true' AND p.created > :cutoff AND r.account in (SELECT * FROM following_table) UNION SELECT p.authorperm, NULL account, p.created t FROM posts p WHERE author NOT IN :accounts AND token = :token AND main_post = 'true' AND p.created > :cutoff AND p.muted = 'false' AND p.author in (SELECT * FROM following_table) ) AS merged %s %s GROUP BY authorperm ORDER BY t desc LIMIT :limit) index INNER JOIN posts p2 ON p2.authorperm = index.authorperm AND p2.token = :token LEFT JOIN post_metadata pm ON p2.authorperm = pm.authorperm"
                    % (timestamp_clause, hive_select_clause)
                ),
                token=token,
                accounts=tuple(accounts),
                last_timestamp=last_timestamp,
                limit=str(limit),
                cutoff=cutoff,
            )
        else:
            return self.db.query(
                (
                    "WITH following_table AS (SELECT following from follows where follower IN :accounts AND state = 1) SELECT p.*, pm.json_metadata FROM posts p LEFT JOIN accounts acc ON p.author = acc.name AND p.token = acc.symbol LEFT JOIN post_metadata pm ON p.authorperm = pm.authorperm WHERE p.author NOT IN :accounts AND p.muted = 'false' AND (acc is NULL OR acc.muted = 'false') AND p.token = :token AND p.main_post = 'true' AND p.created > :cutoff AND p.author in (SELECT * FROM following_table) %s %s ORDER BY p.created desc LIMIT :limit"
                    % (noreblog_timestamp_clause, hive_select_clause)
                ),
                token=token,
                accounts=tuple(accounts),
                last_timestamp=last_timestamp,
                limit=str(limit),
                cutoff=cutoff,
            )

    def get_discussions_by_score(
        self,
        score_key,
        token,
        tag=None,
        limit=100,
        last_authorperm=None,
        main_post=True,
        hive_select=None,
    ):
        last_month = datetime.now() + relativedelta(months=-1)
        tag_clause = ""
        last_score_clause = ""
        hive_select_clause = ""
        extra_conditions = ""

        if tag is not None:
            tag_clause = "AND STRING_TO_ARRAY(p.tags, ',') @> :tags "
        if last_authorperm is not None:
            # without decimal, floating point compare may miss target
            last_score_clause = f"AND p.{score_key} <= (SELECT MAX({score_key}) FROM posts WHERE token = :token AND authorperm in (:last_authorperm, :last_hive_authorperm)) "
        if hive_select is not None:
            if not hive_select or hive_select == "0":
                hive_select_clause = "AND p.authorperm not like 'h@%' "
            else:
                hive_select_clause = "AND p.authorperm like 'h@%' "
        if score_key == "promoted":
            extra_conditions = "AND p.last_payout = '1970-01-01 00:00:00' AND p.promoted > '0' AND p.cashout_time > :current_time"

        q = f"SELECT p.*, pm.json_metadata FROM posts p LEFT JOIN accounts acc ON p.author = acc.name AND p.token = acc.symbol LEFT JOIN post_metadata pm ON p.authorperm = pm.authorperm WHERE p.token = :token AND p.muted = 'false' AND (acc is NULL OR acc.muted = 'false') AND p.main_post = :main_post AND p.created > :cutoff {tag_clause} {last_score_clause} {hive_select_clause} {extra_conditions} ORDER BY {score_key} DESC LIMIT :limit"
        return self.db.query(
            q,
            score_key=score_key,
            tags=[tag],
            token=token,
            last_authorperm=last_authorperm,
            last_hive_authorperm=f"h{last_authorperm}",
            limit=limit,
            main_post=main_post,
            current_time=datetime.utcnow(),
            cutoff=last_month,
        )

    def get_trending_tags(self, token, limit=20):
        q = "SELECT t, sum(total_payout_value) tpv from posts p, unnest(string_to_array(tags, ',')) t Where token = :token and cashout_time > now() - interval '2 weeks' group by 1 ORDER BY 2 DESC LIMIT :limit"
        return [x["t"] for x in self.db.query(q, token=token, limit=limit)]

    def delete_posts(self, authorperm):
        table = self.db[self.__tablename__]
        # self.db.begin()
        for post in table.find(authorperm=authorperm):
            table.delete(authorperm=authorperm, token=post["token"])
            print(f"deleted {authorperm} token {post['token']}")
        # self.db.commit()

    def delete_old_posts(self, days):
        table = self.db[self.__tablename__]
        del_posts = []
        for post in table.find(order_by="created"):
            if (
                datetime.utcnow() - post["created"]
            ).total_seconds() > 60 * 60 * 24 * days:
                del_posts.append(post["authorperm"])
        for post in del_posts:
            table.delete(authorperm=post)

    def delete(self, ID):
        """Delete a data set

        :param int ID: database id
        """
        table = self.db[self.__tablename__]
        table.delete(id=ID)

    def wipe(self, sure=False):
        """Purge the entire database. No data set will survive this!"""
        if not sure:
            log.error(
                "You need to confirm that you are sure "
                "and understand the implications of "
                "wiping your wallet!"
            )
            return
        else:
            table = self.db[self.__tablename__]
            table.drop
