#!/bin/python
import argparse
import re
import sys
from typing import List

import redis
from crontabs import Cron, Tab
from datetime import datetime, timedelta

from befh.instrument import Instrument
from befh.subscription_manager import SubscriptionManager
from befh.util import Logger

class OhlcvProcessor:
    """
    Redis OHLCV Client
    """

    _REDIS_KEY_PREFIX = "befh_"
    _PERIOD_REGEX = re.compile(".*_.*_([0-9]{10})")
    _REDIS_QUEUE_KEY_FORMAT = "%setpq_%s_%s"

    def __init__(self, conn: redis.StrictRedis):
        """
        Constructor
        """
        self.lock = False
        self.conn = conn

    def _parse_trades(self, vals):
        ret = []
        for val in vals:
            str_val = val.decode('utf-8')
            val_split = str_val.split('/')
            price = float(val_split[0])
            volume = float(val_split[1])
            ret.append((price, volume))
        return ret

    def _trades_ohlcv(self, trades):
        if not len(trades):
            return 0, 0, 0, 0, 0

        prices = [x[0] for x in trades]
        volumes = [x[1] for x in trades]

        open = prices[0]
        high = max(prices)
        low = min(prices)
        close = prices[-1]
        volume = sum(volumes)
        return open, high, low, close, volume

    def _queue_key(self, exchange_name, instrument):
        return OhlcvProcessor._REDIS_QUEUE_KEY_FORMAT % (OhlcvProcessor._REDIS_KEY_PREFIX, exchange_name, instrument)

    def process_period(self, queue_key, key):
        threshold_date = datetime.now() - timedelta(seconds=5)
        threshold_epoch = int(threshold_date.strftime("%s"))

        match = OhlcvProcessor._PERIOD_REGEX.match(key)
        if match:
            epoch = int(match.group(1))
            if epoch < threshold_epoch:
                trade_datetime = datetime.fromtimestamp(epoch)

                raw_trades = self.conn.lrange(key, 0, -1)
                trades = self._parse_trades(raw_trades)
                ohlcv = self._trades_ohlcv(trades)
                print(trade_datetime)
                print(ohlcv)

                self.conn.delete(key)
                self.conn.zrem(queue_key, key)

    def process_instmts_queue(self, *args, **kwargs):
        subscription_instmts: List[Instrument] = args[0]
        from_index = args[1]
        to_index = args[2]

        if self.lock:
            return

        self.lock = True

        for subscription_instmt in subscription_instmts:
            exchange_name = subscription_instmt.exchange_name.lower()
            instrument = subscription_instmt.instmt_name.lower()
            queue_key = self._queue_key(exchange_name, instrument)

            keys = self.conn.zrange(queue_key, from_index, to_index)

            for key in keys:
                val = key.decode('utf-8')
                self.process_period(queue_key, val)

        self.lock = False


def main():
    parser = argparse.ArgumentParser(description='Create candles.')
    parser.add_argument('-instmts', action='store', help='Instrument subscription file.', default='subscriptions.ini')
    parser.add_argument('-redis', action='store_true', help='Use Redis.')

    parser.add_argument('-redisdest', action='store', dest='redisdest',
                        help='Redis destination. Formatted as <host:port>')

    parser.add_argument('-redisdb', action='store', dest='redisdb',
                        help='Redis DB. For example \"0\"')
    parser.add_argument('-output', action='store', dest='output',
                        help='Verbose output file path')
    args = parser.parse_args()

    Logger.init_log(args.output)

    is_database_defined = False
    conn = None

    if args.redis:
        host = args.redisdest.split(':')[0]
        port = int(args.redisdest.split(':')[1])
        db = int(args.redisdb)
        conn = redis.StrictRedis(host=host, port=port, db=db)
        is_database_defined = True

    if not is_database_defined:
        print('Error: Please define which database is used.')
        parser.print_help()
        sys.exit(1)

    # Subscription instruments
    if args.instmts is None or len(args.instmts) == 0:
        print('Error: Please define the instrument subscription list. You can refer to subscriptions.ini.')
        parser.print_help()
        sys.exit(1)

    # Initialize subscriptions
    subscription_instmts = SubscriptionManager(args.instmts).get_subscriptions()
    if len(subscription_instmts) == 0:
        print('Error: No instrument is found in the subscription file. ' +
              'Please check the file path and the content of the subscription file.')
        parser.print_help()
        sys.exit(1)

    processor = OhlcvProcessor(conn=conn)

    processor.process_instmts_queue(subscription_instmts, 0, -1)

    # Calculate OHLCV every second
    Cron().schedule(
        Tab(name='calculate_ohlcv').every(seconds=1).run(processor.process_instmts_queue, subscription_instmts, 0, 0)
    ).go()

if __name__ == '__main__':
    main()
