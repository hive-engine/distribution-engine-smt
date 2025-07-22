# This Python file uses the following encoding: utf-8

import logging
from builtins import object

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

timeformat = "%Y%m%d-%H%M%S"


class FollowsDB(object):
    """This is the accounts storage class"""

    __tablename__ = "follows"

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

    def upsert(self, data):
        """Upsert follow data"""
        table = self.db[self.__tablename__]
        table.upsert(data, ["follower", "following"])

    def refresh_follows(self, follower, following_list):
        """Refresh follow state by fetching existing and updating as needed.

        State: 0 - none, 1 - follow, 2 - mute
        """
        try:
            following_set = frozenset(following_list)
            table = self.db[self.__tablename__]
            self.db.begin()
            existing_following_set = frozenset(
                [
                    x["following"]
                    for x in self.db.query(
                        "SELECT following FROM follows WHERE follower=:follower AND state=1",
                        follower=follower,
                    )
                ]
            )
            for missing_following in following_set - existing_following_set:
                table.upsert(
                    dict(follower=follower, following=missing_following, state=1),
                    ["follower", "following"],
                )
            for extra_following in existing_following_set - following_set:
                table.upsert(
                    dict(follower=follower, following=extra_following, state=0),
                    ["follower", "following"],
                )
            self.db.commit()
        except Exception as e:
            print(e)

    def get_following(
        self, follower, following, status, start=None, limit=1000, hive=False
    ):
        status_int = 1 if status == "blog" else 2
        prefixed_follower = f"h@{follower}" if hive else follower
        prefixed_following = f"h@{following}" if hive else following
        if follower is not None:
            follow_clause = " AND follower = :follower"
            start_clause = " AND following >= :start" if start else ""
            order_by_clause = " ORDER BY following"
        else:
            follow_clause = " AND following = :following"
            start_clause = " AND follower >= :start" if start else ""
            order_by_clause = " ORDER BY follower"
        return self.db.query(
            f"SELECT follower, following FROM follows WHERE state=:state {follow_clause} {start_clause} {order_by_clause} LIMIT :limit",
            follower=prefixed_follower,
            following=prefixed_following,
            state=status_int,
            limit=limit,
            start=start,
        )

    def get_follow_count(self, account, hive=False):
        following_count = 0
        follower_count = 0
        prefixed_account = f"h@{account}" if hive else account
        results = self.db.query(
            "SELECT count(following) FROM follows WHERE follower=:account AND state=1",
            account=prefixed_account,
        )
        for result in results:
            following_count = result["count"]
        results = self.db.query(
            "SELECT count(follower) FROM follows WHERE following=:account AND state=1",
            account=prefixed_account,
        )
        for result in results:
            follower_count = result["count"]
        return {
            "account": account,
            "following_count": following_count,
            "follower_count": follower_count,
        }

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
