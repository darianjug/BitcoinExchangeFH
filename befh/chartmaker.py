#!/bin/python
import argparse
import os
import re
import sys
from typing import List

import redis
from crontabs import Cron, Tab
from datetime import datetime, timedelta

from pytz import utc

from befh.instrument import Instrument
from befh.subscription_manager import SubscriptionManager
from befh.util import Logger

import numpy as np
import asciichartpy
import subprocess
import curses
from curses import wrapper



class ChartProcessor:
    """
    Redis Chart Processor
    """

    _REDIS_KEY_PREFIX = "befh_"
    _PERIOD_REGEX = re.compile(".*_.*_([0-9]{10})")
    _REDIS_QUEUE_KEY_FORMAT = "%setpq_%s_%s"
    _REDIS_PRICES_KEY_FORMAT = "%setpr_%s_%s"

    def __init__(self, conn: redis.StrictRedis, stdscr):
        """
        Constructor
        """
        self.lock = False
        self.conn = conn
        self.stdscr = stdscr

    def _parse_prices(self, vals):
        ret = []
        for val in vals:
            str_val = val.decode('utf-8')
            val_split = str_val.split('/')
            epoch = float(val_split[0])
            price = float(val_split[1])
            ret.append((epoch, price))
        return ret

    def _queue_key(self, exchange_name, instrument):
        return ChartProcessor._REDIS_QUEUE_KEY_FORMAT % (ChartProcessor._REDIS_KEY_PREFIX, exchange_name, instrument)

    def _prices_key(self, exchange_name, instrument):
        return ChartProcessor._REDIS_PRICES_KEY_FORMAT % (ChartProcessor._REDIS_KEY_PREFIX, exchange_name, instrument)

    def calculate_chart(self, exchange_name, instrument, from_epoch, to_epoch):
        if not exchange_name == "binance":
            return

        prices_key = self._prices_key(exchange_name, instrument)
        #print(prices_key)

        raw_prices = self.conn.zrangebyscore(prices_key, "(%i" % from_epoch, "(%i" % to_epoch)

        #print("ZRANGEBYSCORE {} {} {}".format(prices_key, "(%i" % from_epoch, "(%i" % to_epoch))

        if not len(raw_prices):
            return
        #print(raw_prices)
        prices = self._parse_prices(raw_prices)
        prices_dict = dict(prices)
        #print(prices_dict)

        ret = []
        last_price = None
        for epoch in range(from_epoch, to_epoch + 1):
            if epoch in prices_dict:
                price = prices_dict[epoch]
                ret.append(price)
                last_price = price
            else:
                if last_price:
                    ret.append(last_price)

        #print(ret)


        #subprocess.call(["printf", "'\033c'"])
        #print('\033[H' + asciichartpy.plot(ret))
        self.stdscr.clear()
        self.stdscr.addstr(0, 0, "%f" % last_price)
        self.stdscr.addstr(1, 0, asciichartpy.plot(ret))
        self.stdscr.refresh()


    def calculate_charts(self, *args, **kwargs):
        subscription_instmts: List[Instrument] = args[0]

        if self.lock:
            return

        self.lock = True

        to_date = datetime.now(tz=utc)
        to_epoch = int(to_date.strftime("%s"))

        from_date = to_date - timedelta(seconds=120)
        from_epoch = int(from_date.strftime("%s"))

        for subscription_instmt in subscription_instmts:
            exchange_name = subscription_instmt.exchange_name.lower()
            instrument = subscription_instmt.instmt_name.lower()
            self.calculate_chart(exchange_name, instrument, from_epoch, to_epoch)

        self.lock = False


def main(stdscr):
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

    stdscr.clear()
    curses.noecho()
    curses.cbreak()
    curses.curs_set(0)
    stdscr.keypad(True)

    processor = ChartProcessor(conn=conn, stdscr=stdscr)

    processor.calculate_charts(subscription_instmts)

    # Calculate chart every second
    Cron().schedule(
        Tab(name='calc_chart').every(seconds=1).run(processor.calculate_charts, subscription_instmts)
    ).go()

if __name__ == '__main__':
    wrapper(main)