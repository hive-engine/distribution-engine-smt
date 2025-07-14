# This Python file uses the following encoding: utf-8
import logging
from builtins import object

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

timeformat = "%Y%m%d-%H%M%S"


class AccountsDB(object):
    """This is the accounts storage class"""

    __tablename__ = "accounts"

    def __init__(self, db):
        self.db = db

    def find(self, symbol):
        table = self.db[self.__tablename__]
        return table.find(symbol=symbol)

    def get(self, name, symbol):
        table = self.db[self.__tablename__]
        return table.find_one(name=name, symbol=symbol)

    def get_all_token(self, name):
        table = self.db[self.__tablename__]
        account = {}
        for data in table.find(name=name):
            account[data["symbol"]] = data
        return account

    def upsert(self, data):
        """Add a new data set"""
        table = self.db[self.__tablename__]
        table.upsert(data, ["name", "symbol"])

    def update_batch(self, data):
        """Add a new data set"""
        table = self.db[self.__tablename__]
        # self.db.begin()
        if isinstance(data, list):
            for d in data:
                table.update(d, ["name", "symbol"])
        else:
            for d in data:
                table.update(data[d], ["name", "symbol"])
        # self.db.commit()

    def update(self, data):
        """Change share_age depending on timestamp"""
        table = self.db[self.__tablename__]
        table.update(data, ["name", "symbol"])

    def get_follow_refresh_time(self, account):
        results = self.db.query(
            "SELECT MAX(last_follow_refresh_time) t FROM accounts WHERE name = :account",
            account=account,
        )
        result = next(results, None)
        return result["t"] if result is not None else None
