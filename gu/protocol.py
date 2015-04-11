# -*- coding: utf-8 -*-
"""参照 twisted/asyncio重新设计下api"""

__author__ = 'wangfei'
__date__ = '2015/03/06'

import gevent.monkey

gevent.monkey.patch_socket()

import logging

import gevent
import gevent.queue

import gevent.pywsgi
import geventwebsocket.handler

from .util import shorten

logger = logging.getLogger(__name__)


def id_generator():
    i = 0
    wall = 1 << 31
    while True:
        i += 1
        if i > wall:
            i = 1
        yield i


class ProtocolException(Exception):
    pass


class Protocol(object):
    """ Server / Client Handler
    """

    id_generator = id_generator()
    read_deadline = 0
    recv_buf_size = 256

    def __init__(self, *args, **kwargs):
        self.sock = args[0]
        self.address = args[1]

        self.sendq = gevent.queue.Queue()  # 发送消息队列

        self.recv_buf_size = kwargs.get('recv_buf_size', self.recv_buf_size)
        self.read_deadline = kwargs.get('read_deadline', self.read_deadline)  # 心跳超时: 0-不超时

        self.session_id = self.id_generator.next()

        self.sender_glet = gevent.spawn(self.loop_sending)
        self.receiver_glet = gevent.spawn(self.loop_recving)

        self.connection_made()

    def connection_made(self):
        logger.info('connection made')

    def data_received(self, data):
        logger.debug('data received: %s', shorten(data, 32))

    def connection_lost(self, reason):
        logger.info('connection lost: %s', reason)
        self.close_protocol()

    def send_data(self, data):
        """异步发送"""
        logger.debug('send data: %s', shorten(data, 32))
        self.sendq.put(data)

    def send_lose(self, data):
        """发送消息然后断开"""
        self.send_rest()
        try:
            self.sock.sendall(data)
        except:
            logger.warn('send lose except', exc_info=1)
        self.close_protocol()

    def send_rest(self):
        """把sendq队列里剩余的发完"""
        while not self.sendq.empty():
            data = self.sendq.get()
            try:
                self.sock.sendall(data)
            except:
                logger.warn('send one except', exc_info=1)
                self.close_protocol()
                break

    def loop_recving(self):
        reason = ''
        while True:
            try:
                if self.read_deadline is not 0:
                    with gevent.Timeout(self.read_deadline, ProtocolException('msg timeout')):
                        data = self.sock.recv(self.recv_buf_size)
                else:
                    data = self.sock.recv(self.recv_buf_size)
            except Exception as e:
                self.sock = None
                if isinstance(e, ProtocolException):
                    reason = 'msg timeout'
                else:
                    reason = 'loop recving except'
                logger.warn('loop recving except', exc_info=1)
                break
            if not data:
                reason = 'loop recving none data'
                break
            self.data_received(data)
        self.connection_lost(reason)

    def loop_sending(self):
        reason = ''
        while True:
            data = self.sendq.get()
            try:
                self.sock.sendall(data)
            except:
                logger.warn('loop sending except', exc_info=1)
                reason = 'loop sending except'
                break
        self.connection_lost(reason)

    def close_protocol(self):
        try:
            self.sender_glet.kill()
        except:
            logger.info('greenlet sender kill except')

        if self.sock:
            self.sock.close()
            self.sock = None

        try:
            self.receiver_glet.kill()
        except:
            logger.info('greenlet receiver kill except')


class HookLogWSGIHandler(gevent.pywsgi.WSGIHandler):
    """ hook gevent.pywsgi.WSGIHandler
        >>> from gevent.pywsgi import WSGIServer
        >>> server = WSGIServer(('127.0.0.1', 6000), app, handler_class=HookLogWSGIHandler)
        >>> server.serve_forever()
    """

    def log_request(self):
        logger.debug(self.format_request())

    def format_request(self):
        length = self.response_length or '-'
        if self.time_finish:
            delta = '%.6f' % (self.time_finish - self.time_start)
        else:
            delta = '-'
        # MARK: 如果使用nginx反向代理,需要根据nginx配置修改client_address为真是ip
        client_address = self.client_address[0] if isinstance(self.client_address, tuple) else self.client_address
        return '%s - - "%s" %s %s %s' % (
            client_address or '-',
            getattr(self, 'requestline', ''),
            (getattr(self, 'status', None) or '000').split()[0],
            length,
            delta)


class HookLogWSHandler(geventwebsocket.handler.WebSocketHandler):
    """ hook geventwebsocket.handler.WebSocketHandler(支持 websocket 的 wsgi handler)
        >>> from gevent.pywsgi import WSGIServer
        >>> server = WSGIServer(('127.0.0.1', 6000), app, handler_class=HookLogWSHandler)
        >>> server.serve_forever()
    """

    def log_request(self):
        logger.debug(self.format_request())

    def format_request(self):
        length = self.response_length or '-'
        if self.time_finish:
            delta = '%.6f' % (self.time_finish - self.time_start)
        else:
            delta = '-'
        # MARK: 如果使用nginx反向代理,需要根据nginx配置修改client_address为真是ip
        client_address = self.client_address[0] if isinstance(self.client_address, tuple) else self.client_address
        return '%s - - "%s" %s %s %s' % (
            client_address or '-',
            getattr(self, 'requestline', ''),
            (getattr(self, 'status', None) or '000').split()[0],
            length,
            delta)

