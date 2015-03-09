# Gevent Server
参照 twisted/asyncio重新设计下api

Usage
--

echo tcp server
	
	from gnet.protocol import TCPServerFactory, Protocol
	from gnet.util import shorten
	import gevent
	import logging
	
	logger = logging.getLogger(__name__)
	logging.basicConfig(level=logging.DEBUG, format='[%(asctime)-15s %(levelname)s:%(module)s] %(message)s')
	
	
	class EchoServerProtocol(Protocol):
	
	    def connection_made(self):
	        logger.info('connection made')
	
	    def data_received(self, data):
	        logger.debug('data received: %s', shorten(data, 32))
            self.send_data(data)
	
	    def connection_lost(self, reason):
	        logger.info('connection lost')
	        super(EchoServerProtocol, self).connection_lost(reason)
	
	
	class EchoServerFactory(TCPServerFactory):
	
	    def build_protocol(self, sock, addr):
	        logger.info('connection handler %s', str(addr))
	        p = EchoServerProtocol(sock, addr)
	        p.factory = self
	        return p
	
	f = EchoServerFactory(('0.0.0.0', 6011))
	f.start()
	
	gevent.wait()

echo tcp reconnecting client

    from gnet.protocol import ReconnectingClientFactory, Protocol
    from gnet.util import shorten
    import gevent
    import logging
    
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.DEBUG, format='[%(asctime)-15s %(levelname)s:%(module)s] %(message)s')
    
    
    class EchoClientProtocol(Protocol):
    
        def connection_made(self):
            logger.info('connection made')
            self.send_data('ooxx')
    
        def data_received(self, data):
            logger.debug('data received: %s', shorten(data, 32))
            self.send_data(data)
            gevent.sleep(2)
    
        def connection_lost(self, reason):
            logger.info('connection lost')
            super(EchoClientProtocol, self).connection_lost(reason)
    
    
    class EchoClientFactory(ReconnectingClientFactory):
        
        reconnect_delay = 10
        
        def build_protocol(self, sock, addr):
            logger.info('connection handler %s', str(addr))
            p = EchoClientProtocol(sock, addr)
            p.factory = self
            return p
    
    f = EchoClientFactory(('127.0.0.1', 6011))
    f.start()
    
    gevent.wait()

sample wsgi server

    # pip install -U bottle
    # pip install -U gevent-websocket
    # pip install -U bottle-websocket

	from gnet.protocol import WSGIServerFactory, HookLogWSHandler
	from bottle.ext.websocket import websocket
	import gevent
	import bottle
	import logging
	
	logger = logging.getLogger(__name__)
	logging.basicConfig(level=logging.DEBUG, format='[%(asctime)-15s %(levelname)s:%(module)s] %(message)s')
	
	app = bottle.Bottle()
	
	@app.route('/', method='GET')
	def index():
	    return 'hello, world!'
	
	@app.route('/ws', apply=[websocket], method='GET')
	def echo(ws):
	    while True:
	        msg = ws.receive()
	        if msg: ws.send(msg)
	
	site = WSGIServerFactory(('0.0.0.0', 6001), app=app, handler_class=HookLogWSHandler)
	site.start()

	gevent.wait()


service

	# -*- coding: utf-8 -*-
	from gnet.protocol import TCPServerFactory, ReconnectingClientFactory, Protocol
	from gnet.util import shorten
	import gevent
	import logging
	
	from service import Service
	
	logger = logging.getLogger(__name__)
	logging.basicConfig(level=logging.DEBUG, format='[%(asctime)-15s %(levelname)s:%(module)s] %(message)s')
	
	
	class EchoServerProtocol(Protocol):
	
	    def connection_made(self):
	        logger.info('connection made')
	
	    def data_received(self, data):
	        logger.debug('data received: %s', shorten(data, 32))
	        self.send_data(data)
	
	    def connection_lost(self, reason):
	        logger.info('connection lost')
	        super(EchoServerProtocol, self).connection_lost(reason)
	
	
	class EchoServerFactory(TCPServerFactory):
	
	    def build_protocol(self, sock, addr):
	        logger.info('connection handler %s', str(addr))
	        p = EchoServerProtocol(sock, addr)
	        p.factory = self
	        return p
	    
	    def on_notify(self, *args, **kwargs):
	        print 'notify ---server---', args
	
	sf = EchoServerFactory(('127.0.0.1', 6011))
	# sf.start()
	
	
	class EchoClientProtocol(Protocol):
	
	    def connection_made(self):
	        logger.info('connection made')
	        self.send_data('ooxx')
	
	    def data_received(self, data):
	        logger.debug('data received: %s', shorten(data, 32))
	        self.send_data(data)
	        gevent.sleep(2)
	
	    def connection_lost(self, reason):
	        logger.info('connection lost')
	        super(EchoClientProtocol, self).connection_lost(reason)
	
	
	class EchoClientFactory(ReconnectingClientFactory):
	
	    reconnect_delay = 10
	
	    def build_protocol(self, sock, addr):
	        logger.info('connection handler %s', str(addr))
	        p = EchoClientProtocol(sock, addr)
	        p.factory = self
	        return p
	        
	    def on_notify(self, *args, **kwargs):
	        print 'notify ---client---', args
	
	cf = EchoClientFactory(('127.0.0.1', 6011))
	# cf.start()
	
	root = Service()
	root.add_factory('echo_server', sf).start()
	root.add_factory('echo_client', cf).start()
	
	root.notify_factory('*', "hello")
	
	gevent.wait()


API
--

Protocol

配置变量

- `recv_buf_size`: recv的参数,一次读取大小, 默认是256, 大小和每条消息大小相当,较为合适
- `read_deadline`: 读心跳超时, 默认为0,不超时

普通方法

- `add_inner_glet`: 把用户开的协程放在Protocol生命周期内,随Protocol关闭而销毁
- `send_data`: 异步发送数据
- `send_lose`: 发送数据后关闭Protocol(连接)
- `close_protocol`: 主动关闭Protocol(连接)

回调方法

- `connection_made`: 连接建立被调用
- `data_received`: 收到数据被调用
- `connection_lost`: 连接关闭被调用
- `pre_connection_lost`: 准备关闭Protocol被调用

Factory

- `build_protocol`: 重载该方法来建立protocol
- `on_notify`: Service 实例向Factory发送消息被调用
- `reconnect_delay`: 在`ReconnectingClientFactory`的重连延时, 默认是0,不重连

Service

- `add_factory`, `get_factory`, `remove_factory`
- `notify_factory`: 向一个或多个Factory发布消息 (观察者模式)
- Service实现了单例模式
- 重载了 `__getitem__`, `__setitem__`, `__delitem__`, 功能和`get_factory`等一样