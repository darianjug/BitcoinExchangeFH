import threading
from io import BytesIO

import pycurl as pycurl
from threading import Lock, BoundedSemaphore

from befh.api_socket import ApiSocket
try:
    import urllib.request as urlrequest
except ImportError:
    import urllib as urlrequest

import json
import ssl

s = pycurl.CurlShare()

class RESTfulApiSocket(ApiSocket):
    """
    Generic REST API call
    """
    DEFAULT_URLOPEN_TIMEOUT = 5
    USER_AGENT = "Mozilla/5.0 (Windows NT 6.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)" + \
                 " Chrome/57.0.2987.133 Safari/537.36"

    def __init__(self, proxy=None):
        """
        Constructor
        """
        ApiSocket.__init__(self, proxy=proxy)

    @classmethod
    def request(cls, url, verify_cert=True, proxy=None):
        """
        Web request
        :param: url: The url link
        :return JSON object
        """
        s.close()

        buffer = BytesIO()
        c = pycurl.Curl()
        c.setopt(c.URL, url)
        c.setopt(c.WRITEDATA, buffer)
        c.setopt(c.CONNECTTIMEOUT, RESTfulApiSocket.DEFAULT_URLOPEN_TIMEOUT)
        c.setopt(c.USERAGENT, RESTfulApiSocket.USER_AGENT)
        c.setopt(c.MAXREDIRS, 5)
        c.setopt(c.LOW_SPEED_LIMIT, 1)
        c.setopt(c.LOW_SPEED_TIME, 600)
        c.setopt(c.FOLLOWLOCATION, 1)
        c.setopt(c.NOSIGNAL, 1)
        c.setopt(c.TIMEOUT_MS, RESTfulApiSocket.DEFAULT_URLOPEN_TIMEOUT * 1000)
        c.setopt(c.SHARE, s)

        if not verify_cert:
            c.setopt(pycurl.SSL_VERIFYPEER, 0)
            c.setopt(pycurl.SSL_VERIFYHOST, 0)

        if proxy is not None:
            c.setopt(pycurl.PROXY, proxy)

        c.perform()
        c.close()


        body = buffer.getvalue()

        try:
            res = json.loads(body.decode('utf-8'))
            return res
        except:
            return {}

    @classmethod
    def parse_l2_depth(cls, instmt, raw):
        """
        Parse raw data to L2 depth
        :param instmt: Instrument
        :param raw: Raw data in JSON
        """
        return None

    @classmethod
    def parse_trade(cls, instmt, raw):
        """
        :param instmt: Instrument
        :param raw: Raw data in JSON
        :return:
        """
        return None

    @classmethod
    def get_order_book(cls, instmt, proxy=None):
        """
        Get order book
        :param instmt: Instrument
        :return: Object L2Depth
        """
        return None

    @classmethod
    def get_trades(cls, instmt, trade_id, proxy=None):
        """
        Get trades
        :param instmt: Instrument
        :param trade_id: Trade id
        :return: List of trades
        """
        return None

