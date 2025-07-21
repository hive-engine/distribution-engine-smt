# This Python file uses the following encoding: utf-8
import logging
from builtins import object

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

timeformat = "%Y%m%d-%H%M%S"


class AccountHistoryTrx(object):
    """This is the trx storage class"""

    __tablename__ = "account_history"

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
        table.upsert(data, ["token", "account", "id"])

    def insert(self, data):
        """Add a new data set"""
        table = self.db[self.__tablename__]
        table.insert(data)

    def add_batch(self, data):
        """Add a new data set"""
        table = self.db[self.__tablename__]

        if isinstance(data, list):
            # table.insert_many(data, chunk_size=chunk_size)
            for d in data:
                table.upsert(d, ["token", "account", "id"])
        else:
            # self.db.begin()
            for d in data:
                table.upsert(data[d], ["token", "account", "id"])

        # self.db.commit()

    def update_batch(self, data):
        """Add a new data set"""
        table = self.db[self.__tablename__]
        # self.db.begin()
        if isinstance(data, list):
            for d in data:
                table.update(d, ["token", "account", "id"])
        else:
            for d in data:
                table.update(data[d], ["token", "account", "id"])
        # self.db.commit()

    def update(self, data):
        """Change share_age depending on timestamp"""
        table = self.db[self.__tablename__]
        table.update(data, ["token", "account", "id"])

    def get_latest(self, token, account):
        table = self.db[self.__tablename__]
        return table.find_one(account=account, token=token, order_by="-id")

    def get_history(self, account, limit=1000, offset=0, hist_type=None, author=None):
        table = self.db[self.__tablename__]
        ret = []
        if limit > 1000:
            limit = 1000

        index = 0
        if hist_type is None and author is None:
            for data in table.find(
                account=account, order_by="-timestamp", _limit=limit + offset
            ):
                if index >= offset and len(ret) < limit:
                    ret.append(data)
                index += 1
        elif hist_type is None and author is not None:
            for data in table.find(
                account=account,
                author=author,
                order_by="-timestamp",
                _limit=limit + offset,
            ):
                if index >= offset and len(ret) < limit:
                    ret.append(data)
                index += 1
        elif hist_type is not None and author is None:
            for data in table.find(
                account=account,
                type=hist_type,
                order_by="-timestamp",
                _limit=limit + offset,
            ):
                if index >= offset and len(ret) < limit:
                    ret.append(data)
                index += 1
        else:
            for data in table.find(
                account=account,
                type=hist_type,
                author=author,
                order_by="-timestamp",
                _limit=limit + offset,
            ):
                if index >= offset and len(ret) < limit:
                    ret.append(data)
                index += 1
        return ret

    def get_token_history(
        self, token, account, limit=1000, offset=0, hist_type=None, author=None
    ):
        table = self.db[self.__tablename__]
        ret = []
        if limit > 1000:
            limit = 1000
        index = 0
        if hist_type is None and author is None:
            for data in table.find(
                account=account,
                token=token,
                order_by="-timestamp",
                _limit=limit + offset,
            ):
                if index >= offset and len(ret) < limit:
                    ret.append(data)
                index += 1
        elif hist_type is None and author is not None:
            for data in table.find(
                account=account,
                token=token,
                author=author,
                order_by="-timestamp",
                _limit=limit + offset,
            ):
                if index >= offset and len(ret) < limit:
                    ret.append(data)
                index += 1
        elif hist_type is not None and author is None:
            for data in table.find(
                account=account,
                token=token,
                type=hist_type,
                order_by="-timestamp",
                _limit=limit + offset,
            ):
                if index >= offset and len(ret) < limit:
                    ret.append(data)
                index += 1
        else:
            for data in table.find(
                account=account,
                token=token,
                type=hist_type,
                author=author,
                order_by="-timestamp",
                _limit=limit + offset,
            ):
                if index >= offset and len(ret) < limit:
                    ret.append(data)
                index += 1
        return ret

    def delete(self, token, account, hist_id):
        table = self.db[self.__tablename__]
        table.delete(token=token, account=account, id=hist_id)

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
