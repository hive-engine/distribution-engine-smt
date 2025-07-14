# This Python file uses the following encoding: utf-8

import logging
from builtins import object

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

timeformat = "%Y%m%d-%H%M%S"


class VotesTrx(object):
    """This is the vote storage class"""

    __tablename__ = "votes"

    def __init__(self, db):
        self.db = db

    def add(self, data):
        """Add a new data set"""
        table = self.db[self.__tablename__]
        table.upsert(data, ["authorperm", "voter", "token"])

    def add_batch(self, data):
        """Add a new data set"""
        table = self.db[self.__tablename__]

        if isinstance(data, list):
            # self.db.begin()
            # table.insert_many(data, chunk_size=chunk_size)
            for d in data:
                # ensure=False, require all indexes be created up front to not waste space
                # Vote primary key lookup doesn't need a redundant index.
                table.upsert(d, ["authorperm", "voter", "token"], ensure=False)
        else:
            # self.db.begin()
            for d in data:
                table.upsert(data[d], ["authorperm", "voter", "token"])

        # self.db.commit()

    def update_batch(self, data):
        """Add a new data set"""
        table = self.db[self.__tablename__]
        # self.db.begin()
        if isinstance(data, list):
            for d in data:
                table.update(d, ["authorperm", "voter", "token"])
        else:
            for d in data:
                table.update(data[d], ["authorperm", "voter", "token"])
        # self.db.commit()

    def update(self, data):
        table = self.db[self.__tablename__]
        table.update(data, ["authorperm", "voter", "token"])

    def get(self, authorperm, voter, token):
        table = self.db[self.__tablename__]
        return table.find_one(authorperm=authorperm, voter=voter, token=token)

    def get_token_vote(self, authorperm, token):
        table = self.db[self.__tablename__]
        votes = []
        for vote in table.find(
            authorperm=authorperm, token=token, order_by="timestamp"
        ):
            votes.append(vote)
        return votes
