# -*- coding: utf-8 -*-
"""gevent connection pool"""

from gevent.event import AsyncResult
from gevent.queue import Queue
import gevent
import logging
import sys

logger = logging.getLogger(__name__)


class ConnectionPool(object):
    """每个线程/连接使用1个队列的连接池(可用在rpc客户端, 数据库客户端等)
    """

    def __init__(self, n, connection_cls, options={}):
        """
        :param connection_cls: 连接客户端类, 最好在 connection_cls 内部实现重连等
        :param options: 连接客户端参数
        """
        self.queues = []
        self.tasks = []
        self.conns = []

        for _ in xrange(n):
            c = connection_cls(**options)
            self.conns.append(c)
            q = Queue()
            self.queues.append(q)
            g = gevent.spawn(self.loop, c, q)
            self.tasks.append(g)

        assert len(self.conns) == n

    def loop(self, c, q):
        """
        :param c: 连接对象
        :param q: q格式: op-操作名(rpc的调用名等) ..
        """
        while True:
            op, args, kwargs, async_result = q.get()
            try:
                rs = getattr(c, op)(*args, **kwargs)
                if async_result:
                    async_result.set(rs)
            except Exception as e:
                logger.error('[Last call]: %s %s', op, str(args), exc_info=1)
                if async_result:
                    async_result.set_exception(Exception(sys.exc_info()[1]))
                else:
                    logger.error(str(e))
            finally:
                pass

    def _selectq(self, qid=-1):
        """选择第几个队列, 默认返回长度最小的队列
        """
        if qid >= 0:
            return self.queues[qid]
        minq = min(self.queues, key=lambda q: q.qsize())
        return minq

    def call(self, op, args=tuple(), kwargs={}, qid=-1, deferred=True):
        """
        :param deferred: 是否返回deferred
        :return: 如果deferred是True, 返回deferred, False返回None
        """
        q = self._selectq(qid)
        if deferred:
            async_result = AsyncResult()
            q.put((op, args, kwargs, async_result))
            return async_result
        else:
            q.put((op, args, kwargs, None))

    def map(self, op, args_lst=[], qid=-1, deferred=True, timeout=None):
        """并发map
        :param args_lst: args list
        :return 返回结果 generator
        """
        deferred_lst = [self.call(op, args, {}, qid=qid, deferred=deferred) for args in args_lst]
        return (d.get(timeout=timeout) for d in deferred_lst)

