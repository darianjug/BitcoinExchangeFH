from befh.ws_api_socket import WebSocketApiClient
from befh.market_data import L2Depth, Trade
from befh.exchanges.gateway import ExchangeGateway
from befh.instrument import Instrument
from befh.util import Logger
from befh.clients.sql_template import SqlClientTemplate
import time
import threading
import json
from functools import partial
from datetime import datetime, timedelta
import pytz
import re
from tzlocal import get_localzone


class ExchGwApiOkexWs(WebSocketApiClient):
    """
    Exchange Socket
    """

    Client_Id = int(time.mktime(datetime.now().timetuple()))

    def __init__(self):
        """
        Constructor
        """
        WebSocketApiClient.__init__(self, 'ExchApiHuoBi')

    @classmethod
    def get_bids_field_name(cls):
        return 'bids'

    @classmethod
    def get_asks_field_name(cls):
        return 'asks'

    @classmethod
    def get_link(cls):
        return 'wss://real.okex.com:10441/websocket'

    @classmethod
    def get_order_book_subscription_string(cls, instmt):
        return json.dumps({"event":"addChannel", "channel": instmt.get_order_book_channel_id()})

    @classmethod
    def get_trades_subscription_string(cls, instmt):
        return json.dumps({"event":"addChannel", "channel": instmt.get_trades_channel_id()})

    @classmethod
    def parse_l2_depth(cls, instmt, raw):
        """
        Parse raw data to L2 depth
        :param instmt: Instrument
        :param raw: Raw data in JSON
        """
        l2_depth = instmt.get_l2_depth()
        keys = list(raw.keys())
        if cls.get_bids_field_name() in keys and \
           cls.get_asks_field_name() in keys:

            # Date time
            timestamp = raw['timestamp']
            l2_depth.date_time = datetime.utcfromtimestamp(timestamp/1000.0).strftime("%Y%m%d %H:%M:%S.%f")

            # Bids
            bids = raw[cls.get_bids_field_name()]
            bids_len = min(l2_depth.depth, len(bids))
            for i in range(0, bids_len):
                l2_depth.bids[i].price = float(bids[i][0]) if type(bids[i][0]) != float else bids[i][0]
                l2_depth.bids[i].volume = float(bids[i][1]) if type(bids[i][1]) != float else bids[i][1]

            # Asks
            asks = raw[cls.get_asks_field_name()]
            asks_len = min(l2_depth.depth, len(asks))
            for i in range(0, asks_len):
                l2_depth.asks[i].price = float(asks[i][0]) if type(asks[i][0]) != float else asks[i][0]
                l2_depth.asks[i].volume = float(asks[i][1]) if type(asks[i][1]) != float else asks[i][1]
        else:
            raise Exception('Does not contain order book keys in instmt %s-%s.\nOriginal:\n%s' % \
                (instmt.get_exchange_name(), instmt.get_instmt_name(), \
                 raw))
        return l2_depth

    @classmethod
    def parse_trade(cls, instmt, raws):
        """
        :param instmt: Instrument
        :param raw: Raw data in JSON
        :return:
        """

        trades = []
        for item in raws:
            trade = Trade()
            today = datetime.today().date()
            time = item[3]

            timestamp_split = time.split(':')
            hour = int(timestamp_split[0])
            minute = int(timestamp_split[1])
            second = int(timestamp_split[2])

            trade_date = datetime.now(pytz.timezone('Asia/Shanghai'))
            trade_date = trade_date.replace(hour=hour, minute=minute, second=second)

            now = datetime.now(pytz.timezone('Asia/Shanghai'))

            if trade_date > now:
                trade_date = trade_date - timedelta(1)

            trade.date_time = trade_date.astimezone(pytz.UTC).strftime("%Y%m%d %H:%M:%S.%f")

            # Buy = 0
            # Side = 1
            trade.trade_side = Trade.parse_side(item[4])
            # Trade id
            trade.trade_id = str(item[0])
            # Trade price
            trade.trade_price = item[1]
            # Trade volume
            trade.trade_volume = item[2]
            trades.append(trade)
        return trades


class ExchGwOkex(ExchangeGateway):
    """
    Exchange gateway
    """
    def __init__(self, db_clients):
        """
        Constructor
        :param db_client: Database client
        """
        ExchangeGateway.__init__(self, ExchGwApiOkexWs(), db_clients)

    @classmethod
    def get_exchange_name(cls):
        """
        Get exchange name
        :return: Exchange name string
        """
        return 'Okex'

    def on_open_handler(self, instmt, ws):
        """
        Socket on open handler
        :param instmt: Instrument
        :param ws: Web socket
        """
        Logger.info(self.__class__.__name__, "Instrument %s is subscribed in channel %s" % \
                  (instmt.get_instmt_name(), instmt.get_exchange_name()))
        if not instmt.get_subscribed():
            instmt.set_order_book_channel_id("ok_sub_%s_depth" % instmt.get_instmt_code())
            instmt.set_trades_channel_id("ok_sub_%s_deals" % instmt.get_instmt_code())

            Logger.info(self.__class__.__name__, 'order book string:{}'.format(self.api_socket.get_order_book_subscription_string(instmt)))
            Logger.info(self.__class__.__name__, 'trade string:{}'.format(self.api_socket.get_trades_subscription_string(instmt)))
            ws.send(self.api_socket.get_order_book_subscription_string(instmt))
            ws.send(self.api_socket.get_trades_subscription_string(instmt))
            instmt.set_subscribed(True)

    def on_close_handler(self, instmt, ws):
        """
        Socket on close handler
        :param instmt: Instrument
        :param ws: Web socket
        """
        Logger.info(self.__class__.__name__, "Instrument %s is unsubscribed in channel %s" % \
                  (instmt.get_instmt_name(), instmt.get_exchange_name()))
        instmt.set_subscribed(False)

    def on_message_handler(self, instmt, message):
        """
        Incoming message handler
        :param instmt: Instrument
        :param message: Message
        """
        for item in message:
            if 'channel' in item:
                channel = item['channel']

                if channel == instmt.get_order_book_channel_id():
                    instmt.set_prev_l2_depth(instmt.get_l2_depth().copy())
                    self.api_socket.parse_l2_depth(instmt, item['data'])
                    if instmt.get_l2_depth().is_diff(instmt.get_prev_l2_depth()):
                        instmt.incr_order_book_id()
                        self.insert_order_book(instmt)
                elif channel == instmt.get_trades_channel_id():
                    trades = self.api_socket.parse_trade(instmt, item['data'])
                    for trade in trades:
                        if trade.trade_id != instmt.get_exch_trade_id():
                            instmt.incr_trade_id()
                            instmt.set_exch_trade_id(trade.trade_id)
                            self.insert_trade(instmt, trade)

    def start(self, instmt):
        """
        Start the exchange gateway
        :param instmt: Instrument
        :return List of threads
        """
        instmt.set_l2_depth(L2Depth(20))
        instmt.set_prev_l2_depth(L2Depth(20))
        instmt.set_instmt_snapshot_table_name(self.get_instmt_snapshot_table_name(instmt.get_exchange_name(),
                                                                                  instmt.get_instmt_name()))
        instmt.set_instmt_trades_table_name(self.get_instmt_trades_table_name(instmt.get_exchange_name(),
                                                                              instmt.get_instmt_name()))
        self.init_instmt_snapshot_table(instmt)
        self.init_instmt_trades_table(instmt)
        Logger.info(self.__class__.__name__, 'instmt snapshot table: {}'.format(instmt.get_instmt_snapshot_table_name()))
        return [self.api_socket.connect(self.api_socket.get_link(),
                                        on_message_handler=partial(self.on_message_handler, instmt),
                                        on_open_handler=partial(self.on_open_handler, instmt),
                                        on_close_handler=partial(self.on_close_handler, instmt))]

if __name__ == '__main__':
    import logging
    import websocket
    websocket.enableTrace(True)
    logging.basicConfig()
    Logger.init_log()
    exchange_name = 'Okex'
    instmt_name = 'BTC'
    instmt_code = 'btc'
    instmt = Instrument(exchange_name, instmt_name, instmt_code)
    db_client = SqlClientTemplate()
    exch = ExchGwOkex([db_client])
    td = exch.start(instmt)
    pass
