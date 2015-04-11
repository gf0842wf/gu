#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" test server and client"""

from gu.protocol import Protocol
import gevent
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG, format='[%(asctime)-15s %(levelname)s:%(module)s] %(message)s')


class EchoServerProtocol(Protocol):
    def __init__(self, sock, address):
        self.read_deadline = 20
        super(EchoServerProtocol, self).__init__(sock, address)

    def connection_made(self):
        logger.info('connection made. session_id: %s', self.session_id)

    def data_received(self, data):
        logger.debug('data received: %s', data)
        self.send_data(data)

    def connection_lost(self, reason):
        logger.info('connection lost')
        super(EchoServerProtocol, self).connection_lost(reason)


from gevent.server import StreamServer

server = StreamServer(('0.0.0.0', 6000), EchoServerProtocol)
gevent.spawn(server.serve_forever)


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


from gevent.socket import create_connection

s = create_connection(('127.0.0.1', 6000))
gevent.spawn_later(5, EchoClientProtocol, s, None)

gevent.wait()
