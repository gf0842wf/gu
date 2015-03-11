#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" description
mysql db connection and pool for gevent
mysql版本必须大于5.1, 否则无法使用 事务(autocommit)
当使用事务时,同一个事务,必须制定一个qid
"""

__author__ = 'wangfei'
__date__ = '2015/03/06'

from gevent import monkey

monkey.patch_socket()

from gevent.queue import Queue
from gevent.event import AsyncResult
import sys
import gevent
import umysql
import socket
import traceback
import logging

logger = logging.getLogger(__name__)


class Connection(object):
    reconnect_delay = 0  # 重连等待时间, 0-不重连

    def __init__(self, host, user, passwd, db, port=3306, autocommit=True, charset='utf8'):
        self.args = (host, port, user, passwd, db, autocommit, charset)
        self.conn = umysql.Connection()
        self.conn.connect(*self.args)

    def reconnect(self, delay):
        while True:
            self.conn.close()
            self.conn = umysql.Connection()
            try:
                logger.info('trying reconnect..')
                self.conn.connect(*self.args)
                logger.info('reconnected.')
                break
            except:
                logger.error('reconnect except', exc_info=1)
            gevent.sleep(delay)

    def query(self, sql, args):
        """
        :return: 返回False-表示不重连的时候查询失败
        """
        logger.debug('sql: %s, args: %s', sql, str(args))
        if args:
            assert isinstance(args, (tuple, list))
        try:
            return self.conn.query(sql, args)
        except socket.error:
            logger.warn('[sending query]: %s', sql, exc_info=1)
            if self.reconnect_delay is not 0:
                self.reconnect(self.reconnect_delay)
                return self.conn.query(sql, args)
            return False
        except:
            if not self.conn.is_connected():
                logger.warn('[sending query]: %s', sql, exc_info=1)
                if self.reconnect_delay is not 0:
                    self.reconnect(self.reconnect_delay)
                    return self.conn.query(sql, args)
                return False
            else:
                raise


class PoolError(Exception):
    pass


class Pool(object):
    """[连接池] 每个连接使用一个gevent队列的连接池
    : 队列格式: (sql, args, )
    """

    def __init__(self, kwargs, n, reconnect_delay):
        assert n > 0, n
        self.conns = []
        self.queues = []
        self.tasks = []

        for _ in xrange(n):
            c = Connection(**kwargs)
            c.reconnect_delay = reconnect_delay
            self.conns.append(c)
            q = Queue()
            self.queues.append(q)
            g = gevent.spawn(self.loop, c, q)
            self.tasks.append(g)

        assert len(self.conns) == n

    def get_result_rows(self, rs, curclass):
        if not curclass or curclass == 'dict':
            fields = [row[0] for row in rs.fields]
            return [dict(zip(fields, row)) for row in rs.rows]
        else:
            return rs.rows

    def loop(self, conn, q):
        """
        :param q, 队列格式: (sql, args, op, curclass, result) .
        op是操作类型, 0是execute, 1是fetchone, 2是fetchall, 3是获得结果字段名列表.
        curclass是结果集类型, 默认是dict(字典), 也可以是list(列表).
        result: 是gevent的AsyncResult对象, result为空则非阻塞
        """
        while 1:
            sql, args, op, curclass, result = q.peek()
            try:
                rs = conn.query(sql, args)
                if result:
                    if op == 0:
                        result.set(rs[1] or rs[0])
                    elif op == 1:
                        rows = self.get_result_rows(rs, curclass)
                        row = rows and rows[0] or None
                        result.set(row)
                    elif op == 2:
                        rows = self.get_result_rows(rs, curclass)
                        result.set(rows)
                    elif op == 3:
                        fields = [row[0] for row in rs.fields]
                        result.set(fields)
                    else:
                        raise PoolError('query op is wrong. %s' % op)
            except:
                logger.error('[last query]: %s, %s', sql, str(args), exc_info=1)
                if result:
                    result.set_exception(sys.exc_info()[1])
                else:
                    logger.error(str(traceback.format_exc()))
            finally:
                q.next()

    def _selectq(self, qid=-1):
        """选择第几个队列, 默认返回长度最小的队列
        """
        if qid >= 0:
            return self.queues[qid]
        minq = min(self.queues, key=lambda qs: qs.qsize())
        return minq

    def query(self, sql, args=[], op=0, qid=-1, curclass=None, block=True):
        if not isinstance(qid, (int, long)):
            qid = -1
        q = self._selectq(qid)
        if block:
            result = AsyncResult()
            q.put((sql, args, op, curclass, result))
            return result.get()
        else:
            q.put((sql, args, op, curclass, None))

    def execute(self, sql, args=[], qid=-1, curclass=None, block=True):
        return self.query(sql, args, 0, qid, curclass, block)

    def fetchone(self, sql, args=[], qid=-1, curclass=None, block=True):
        return self.query(sql, args, 1, qid, curclass, block)

    def fetchall(self, sql, args=[], qid=-1, curclass=None, block=True):
        return self.query(sql, args, 2, qid, curclass, block)

    def get_fields(self, tbname):
        return self.query('select * from %s limit 0' % tbname, [], 3, qid=-1, curclass=None, block=True)


if __name__ == '__main__':
    kwargs = dict(host='localhost', user='root', passwd='112358', db='test')
    pool = Pool(kwargs, 20, 0)

    print pool.fetchall('select * from book where author = "%s"' % 'fk')

    # 像 execute 如果不关心执行结果,可以异步执行
    pool.execute('insert into book set name="abc", author="fk"', block=False)

    gevent.wait()
