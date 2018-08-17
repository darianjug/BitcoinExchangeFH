import json
from datetime import datetime

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
    trades_table_regex = re.compile("exch_.*_.*_trades_[0-9]{8}")
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
        if table == "exchanges_snapshot":
            ret = dict(zip(columns, values))
            ret['table'] = table
            json_ret = json.dumps(ret)
            exchange = ret['exchange']
            instrument = ret['instmt']

            self.lock.acquire()
            for column in columns:
                key = "%s_%s_%s" % (exchange, instrument, column)
                value = ret[column]
                self.conn.set(key, value)

            self.conn.publish("bitcoin_exchange_fh", json_ret)
            self.lock.release()
        elif RedisClient.trades_table_regex.match(table):
            ret = dict(zip(columns, values))
            ret['table'] = table
            exchange = "exchange"
            instrument = "instrument"
            trade_price = str(ret['trade_price'])
            trade_volume = str(ret['trade_volume'])

            trade_date = datetime.strptime(ret['date_time'], '%Y%m%d %H:%M:%S.%f')

            year = trade_date.year
            month = trade_date.month
            day = trade_date.day
            hour = trade_date.hour
            minute = trade_date.minute
            second = trade_date.second



            lkey = "%s_%s_%d%d%d_%d%d%d" % (exchange, instrument, year, month, day, hour, minute, second)
            val = "/".join([trade_price, trade_volume])
            self.conn.lpush(lkey, val)

            print(trade_date)
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