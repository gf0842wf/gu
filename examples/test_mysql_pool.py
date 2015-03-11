#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" description
test gevent mysql pool
"""

__author__ = 'wangfei'
__date__ = '2015/03/11'

from gnet.db.gmysql import Pool
import gevent

options = dict(host='localhost', user='root', passwd='112358', db='test', reconnect_delay=5)
pool = Pool(options, 20)

print pool.fetchall('select * from book where author = "%s"' % 'fk')

# 像 execute 如果不关心执行结果,可以异步执行
pool.execute('insert into book set name="abc", author="fk"', block=False)

gevent.wait()