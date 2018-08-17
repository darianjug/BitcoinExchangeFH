#!/bin/python
import argparse
import sys

import redis
from crontabs import Cron, Tab
from datetime import datetime, timedelta

from befh.util import Logger

def parse_trades(vals):
    ret = []
    for val in vals:
        str_val = val.decode('utf-8')
        val_split = str_val.split('/')
        price = float(val_split[0])
        volume = float(val_split[1])
        ret.append((price, volume))
    return ret

def get_ohlcv(trades):
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

def my_job(*args, **kwargs):
    conn: redis.StrictRedis = kwargs['conn']
    exchange = "exchange"
    instrument = "instrument"

    now = datetime.utcnow() - timedelta(seconds=1)
    year = now.year
    month = now.month
    day = now.day
    hour = now.hour
    minute = now.minute
    second = now.second

    lkey = "%s_%s_%d%d%d_%d%d%d" % (exchange, instrument, year, month, day, hour, minute, second)

    val = conn.lrange(lkey, 0, -1)
    trades = parse_trades(val)
    Logger.info('[main]', get_ohlcv(trades))


def main():
    parser = argparse.ArgumentParser(description='Create candles.')
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

    # Will run with a 5 second interval synced to the top of the minute
    Cron().schedule(
        Tab(name='run_my_job').every(seconds=1).run(my_job, 'my_arg', conn=conn)
    ).go()

if __name__ == '__main__':
    main()
