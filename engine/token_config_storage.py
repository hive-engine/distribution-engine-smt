# This Python file uses the following encoding: utf-8
import logging
from builtins import object

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

timeformat = "%Y%m%d-%H%M%S"


class TokenConfigDB(object):
    """This is the accounts storage class"""

    __tablename__ = "token_config"

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

    def get_all(self):
        """Returns the public keys stored in the database"""
        table = self.db[self.__tablename__]
        data = {}
        for token in table.all():
            data[token["token"]] = token
        return data

    def get_all_list(self):
        """Returns the public keys stored in the database"""
        table = self.db[self.__tablename__]
        data = []
        for token in table.all():
            data.append(token)
        return data

    def get(self, symbol):
        table = self.db[self.__tablename__]
        return table.find_one(token=symbol)

    def upsert(self, data):
        """Add a new data set"""
        table = self.db[self.__tablename__]
        table.upsert(data, ["token"])
        # self.db.commit()

    def update(self, data):
        """Change share_age depending on timestamp"""
        table = self.db[self.__tablename__]
        table.update(data, ["token"])

    def delete(self, symbol):
        """Delete a data set

        :param int ID: database id
        """
        table = self.db[self.__tablename__]
        table.delete(symbol=symbol)

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
