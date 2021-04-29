# This Python file uses the following encoding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from builtins import bytes
from builtins import object
from beemgraphenebase.py23 import py23_bytes, bytes_types
from sqlalchemy.dialects.postgresql import insert as pg_insert
import shutil
import time
import os
import json
import sqlite3
from appdirs import user_data_dir
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from beem.utils import formatTimeString, addTzInfo
import logging
from binascii import hexlify
import random
import hashlib
import dataset
from sqlalchemy import and_
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

timeformat = "%Y%m%d-%H%M%S"

class PostMetadataStorage(object):
    """ This is the post metadata storage class
    """
    __tablename__ = 'post_metadata'

    def __init__(self, db):
        self.db = db

    def exists_table(self):
        """ Check if the database table exists
        """

        if len(self.db.tables) == 0:
            return False
        if self.__tablename__ in self.db.tables:
            return True
        else:
            return False
 
    def add(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        table.upsert(data, ["authorperm"])

    def add_batch(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        #self.db.begin()
        if isinstance(data, list):
            #table.insert_many(data, chunk_size=chunk_size)
            for d in data:
                table.upsert(d, ["authorperm"])
        else:
            for d in data:
                table.upsert(data[d], ["authorperm"])            
            
        #self.db.commit()

    def update_batch(self, data):
        """ Add a new data set

        """
        table = self.db[self.__tablename__]
        #self.db.begin()
        if isinstance(data, list):
            for d in data:
                table.update(d, ["authorperm"])
        else:
            for d in data:
                table.update(data[d], ["authorperm"])            
        #self.db.commit()

    def update(self, data):
        table = self.db[self.__tablename__]
        table.update(data, ['authorperm'])


    def upsert(self, data):
        table = self.db[self.__tablename__]
        table.upsert(data, ['authorperm'])

    def get(self, authorperm):
        table = self.db[self.__tablename__]
        return table.find_one(authorperm=authorperm)

