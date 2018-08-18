import json
from datetime import datetime

import pytz
from pytz import utc

from befh.clients.database import DatabaseClient
from befh.util import Logger
import threading
import re
import zmq
import time
import redis
import re

class RedisClient(DatabaseClient):
    """
    Redis Client
    """

    _REDIS_KEY_PREFIX = "befh_"
    _EXCHANGES_SNAPSHOT_TABLE_NAME = "exchanges_snapshot"
    _TRADES_TABLE_PREFIX = re.compile("exch_(.*)_(.*)_trades_[0-9]{8}")

    def __init__(self):
        """
        Constructor
        """
        DatabaseClient.__init__(self)
        self.lock = threading.Lock()

    def connect(self, **kwargs):
        """
        Connect
        :param path: sqlite file to connect
        """
        host = kwargs['host']
        port = kwargs['port']
        db = kwargs['db']
        self.conn = redis.StrictRedis(host=host, port=port, db=db)
        return self.conn is not None


    def execute(self, sql):
        """
        Execute the sql command
        :param sql: SQL command
        """
        return True

    def commit(self):
        """
        Commit
        """
        return True

    def fetchone(self):
        """
        Fetch one record
        :return Record
        """
        return []

    def fetchall(self):
        """
        Fetch all records
        :return Record
        """
        return []

    def create(self, table, columns, types, primary_key_index=(), is_ifnotexists=True):
        """
        Create table in the database.
        Caveat - Assign the first few column as the keys!!!
        :param table: Table name
        :param columns: Column array
        :param types: Type array
        :param is_ifnotexists: Create table if not exists keyword
        """
        return True

    def insert(self, table, columns, types, values, primary_key_index=(), is_orreplace=False, is_commit=True):
        """
        Insert into the table
        :param table: Table name
        :param columns: Column array
        :param types: Type array
        :param values: Value array
        :param primary_key_index: An array of indices of primary keys in columns,
                          e.g. [0] means the first column is the primary key
        :param is_orreplace: Indicate if the query is "INSERT OR REPLACE"
        """

        # If it's an exchange snapshot table SET all column values into Redis and also publish the values as JSON.
        if table == RedisClient._EXCHANGES_SNAPSHOT_TABLE_NAME:
            ret = dict(zip(columns, values))
            ret['table'] = table
            json_ret = json.dumps(ret)

            exchange = ret['exchange']
            instrument = ret['instmt']

            self.lock.acquire()
            for column in columns:
                key = "%ses_%s_%s_%s" % (RedisClient._REDIS_KEY_PREFIX,
                                         exchange.lower(),
                                         instrument.lower(),
                                         column.lower())
                value = ret[column]
                self.conn.set(key, value)

            self.conn.publish("%ses" % RedisClient._REDIS_KEY_PREFIX, json_ret)
            self.lock.release()
        else:
            # If it's the trades table add the amount and volume to the period trades list and add the period to the
            # period queue.
            trades_table_match = RedisClient._TRADES_TABLE_PREFIX.match(table)
            if trades_table_match:
                ret = dict(zip(columns, values))
                ret['table'] = table

                exchange = trades_table_match.group(1)
                instrument = trades_table_match.group(2)

                trade_price = str(ret['trade_price'])
                trade_volume = str(ret['trade_volume'])

                trade_date = pytz.utc.localize(datetime.strptime(ret['date_time'], '%Y%m%d %H:%M:%S.%f'))
                epoch = int(trade_date.strftime("%s"))

                now = datetime.now(tz=utc)

                # Delay between now and trade date.
                print("%{}".format(now - trade_date))

                period_key = "%setp_%s_%s_%d" % (RedisClient._REDIS_KEY_PREFIX,
                                                 exchange,
                                                 instrument,
                                                 epoch)
                queue_key = "%setpq_%s_%s" % (RedisClient._REDIS_KEY_PREFIX,
                                              exchange,
                                              instrument)

                val = "/".join([trade_price, trade_volume])

                self.lock.acquire()
                self.conn.lpush(period_key, val)
                self.conn.zadd(queue_key, epoch, period_key)
                self.lock.release()
        return True

    def select(self, table, columns=['*'], condition='', orderby='', limit=0, isFetchAll=True):
        """
        Select rows from the table
        :param table: Table name
        :param columns: Selected columns
        :param condition: Where condition
        :param orderby: Order by condition
        :param limit: Rows limit
        :param isFetchAll: Indicator of fetching all
        :return Result rows
        """
        return []

    def delete(self, table, condition='1==1'):
        """
        Delete rows from the table
        :param table: Table name
        :param condition: Where condition
        """
        return True