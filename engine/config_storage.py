# This Python file uses the following encoding: utf-8

import logging
from builtins import object

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

timeformat = "%Y%m%d-%H%M%S"

HIVED = 1
ENGINE_SIDECHAIN = 2


class ConfigurationDB(object):
    """This is the trx storage class"""

    __tablename__ = "configuration"

    def __init__(self, db):
        self.db = db

    def get(self):
        table = self.db[self.__tablename__]
        return table.find_one(id=HIVED)

    def get_engine(self):
        table = self.db[self.__tablename__]
        return table.find_one(id=ENGINE_SIDECHAIN)

    def upsert(self, data):
        data["id"] = HIVED
        table = self.db[self.__tablename__]
        table.upsert(data, ["id"])

    def upsert_engine(self, data):
        data["id"] = ENGINE_SIDECHAIN
        table = self.db[self.__tablename__]
        table.upsert(data, ["id"])
