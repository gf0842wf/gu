# -*- coding: utf-8 -*-
from gnet.protocol import Protocol
import gevent
import logging

from gevent.socket import create_connection

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='[%(asctime)-15s %(levelname)s:%(module)s] %(message)s')


class EchoClientProtocol(Protocol):
    def connection_made(self):
        logger.info('connection made')
        self.send_data('ooxx')

    def data_received(self, data):
        logger.debug('data received: %s', data)
        self.send_data(data)
        gevent.sleep(2)

    def connection_lost(self, reason):
        logger.info('connection lost')
        super(EchoClientProtocol, self).connection_lost(reason)

        reconnect()


def reconnect():
    while True:
        logger.info('try reconnect..')
        try:
            s = create_connection(('127.0.0.1', 6000))
        except:
            gevent.sleep(5)
            continue
        logger.info('reconnected.')
        gevent.spawn(EchoClientProtocol, s, None)
        break


s = create_connection(('127.0.0.1', 6000))
gevent.spawn(EchoClientProtocol, s, None)

gevent.wait()