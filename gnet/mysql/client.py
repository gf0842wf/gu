#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" description
mysql db client (可以非gevent使用,如果使用gevent,需要程序入口文件patch all)
兼容 ultramysql, MySQLdb, pymysql
"""

__author__ = 'wangfei'
__date__ = '2015/03/11'

import logging
import time

logger = logging.getLogger(__name__)

try:
    import MySQLdb
    import MySQLdb.cursors
except ImportError:
    logger.warn('MySQLdb module not found.')


class MySQLdbConnection(object):
    def __init__(self, host, user, passwd, db, port=3306, autocommit=True, charset='utf8', reconnect_delay=0):
        """
        :param reconnect_delay: 重连等待时间, 0-不重连
        """
        self.args = dict(passwd=passwd, user=user, charset=charset, db=db)
        if '/' in host:
            self.args['unix_socket'] = host
        else:
            self.args['host'] = host
            self.args['port'] = port
        self.reconnect_delay = reconnect_delay
        self.conn = MySQLdb.Connection(**self.args)
        self.autocommit = autocommit
        if self.autocommit:
            self.conn.autocommit(True)

    def reconnect(self):
        while True:
            try:
                self.conn.close()
            except:
                pass
            try:
                logger.info('trying reconnect..')
                self.conn = pymysql.Connection(**self.args)
                if self.autocommit:
                    self.conn.autocommit(True)
                logger.info('reconnected.')
                break
            except:
                logger.error('reconnect except', exc_info=1)
            time.sleep(self.reconnect_delay)

    def execute(self, query, *args, **kwargs):
        """Executes the given query, returning the lastrowid from the query."""
        return self.execute_lastrowid(query, *args, **kwargs)

    def execute_lastrowid(self, query, *args, **kwargs):
        """Executes the given query, returning the lastrowid from the query."""
        try:
            result, cursor = self._execute(query, args, kwargs)
            if result is False:
                return False
            return cursor.lastrowid
        finally:
            if locals().get('cursor'):
                cursor.close()

    def fetchall(self, query, *args, **kwargs):
        """Returns a row list for the given query and args."""
        try:
            result, cursor = self._execute(query, args, kwargs)
            column_names = [d[0] for d in cursor.description]
            if result is False:
                return False
            return [Row(zip(column_names, row)) for row in cursor]
        finally:
            if locals().get('cursor'):
                cursor.close()

    def fetchone(self, query, *args, **kwargs):
        """Returns the (singular) row returned by the given query.
        If the query has no results, returns None.  If it has
        more than one result, raises an exception.
        """
        rows = self.fetchall(query, *args, **kwargs)
        if rows is False:
            return False
        elif not rows:
            return None
        elif len(rows) > 1:
            logger.warn('Multiple rows returned for fetchone')
            return rows[0]
        else:
            return rows[0]

    def executemany(self, query, args):
        """Executes the given query against all the given param sequences.
        We return the lastrowid from the query.
        example:
        executemany('insert into book (name, author) values (%s, %s)',
                    [
                        ('a', u'张三'),
                        ('b', u'李四'),
                        ('c', u'王二')])
        """
        return self.executemany_lastrowid(query, args)

    def executemany_lastrowid(self, query, args):
        """Executes the given query against all the given param sequences.
        We return the lastrowid from the query.
        """
        try:
            result, cursor = self._executemany(query, args)
            if result is False:
                return False
            return cursor.lastrowid
        finally:
            if locals().get('cursor'):
                cursor.close()

    def _execute(self, query, args, kwargs):
        """
        :return: [result, cursor], result: False-表示不重连的时候查询失败(或者是重练成功后又执行失败)
        """
        cursor = self.conn.cursor()
        try:
            logger.debug('sql: %s, args: %s', query, str(args))
            return [cursor.execute(query, args or kwargs), cursor]
        except:
            logger.warn('[Error query]: %s args: %s', query, str(args), exc_info=1)
            if self.reconnect_delay > 0:
                self.reconnect()
                cursor.close()
                try:
                    cursor = self.conn.cursor()
                    return [cursor.execute(query, args or kwargs), cursor]
                except:
                    logger.error('[Error query]:sql: %s args: %s', query, str(args), exc_info=1)
                    cursor.close()
                    return [False, cursor]
            else:
                logger.error('[Error query]:sql: %s args: %s. Not reconnect', query, str(args), exc_info=1)
                try:
                    self.conn.close()
                except:
                    pass
                cursor.close()
                return [False, cursor]

    def _executemany(self, query, args):
        """
        :return: [result, cursor], result: False-表示不重连的时候查询失败(或者是重练成功后又执行失败)
        """
        cursor = self.conn.cursor()
        try:
            logger.debug('sql: %s, args: %s', query, str(args))
            return [cursor.executemany(query, args), cursor]
        except:
            logger.warn('[Error query]: %s args: %s', query, str(args), exc_info=1)
            if self.reconnect_delay > 0:
                self.reconnect()
                cursor.close()
                try:
                    cursor = self.conn.cursor()
                    return [cursor.executemany(query, args), cursor]
                except:
                    logger.error('[Error query]:sql: %s args: %s', query, str(args), exc_info=1)
                    cursor.close()
                    return [False, cursor]
            else:
                logger.error('[Error query]:sql: %s args: %s. Not reconnect', query, str(args), exc_info=1)
                try:
                    self.conn.close()
                except:
                    pass
                cursor.close()
                return [False, cursor]

    def get_fields(self, table_name):
        result, cursor = self._execute('select * from %s limit 0' % table_name, tuple(), tuple())
        if result is False:
            return False
        return [i[0] for i in cursor.description]


class Row(dict):
    """A dict that allows for object-like property access syntax."""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)


if MySQLdb is not None:
    # Fix the access conversions to properly recognize unicode/binary
    import copy
    import MySQLdb.constants
    import MySQLdb.converters

    FIELD_TYPE = MySQLdb.constants.FIELD_TYPE
    FLAG = MySQLdb.constants.FLAG
    CONVERSIONS = copy.copy(MySQLdb.converters.conversions)

    field_types = [FIELD_TYPE.BLOB, FIELD_TYPE.STRING, FIELD_TYPE.VAR_STRING]
    if 'VARCHAR' in vars(FIELD_TYPE):
        field_types.append(FIELD_TYPE.VARCHAR)

    for field_type in field_types:
        CONVERSIONS[field_type] = [(FLAG.BINARY, str)] + CONVERSIONS[field_type]

    # Alias some common MySQL exceptions
    IntegrityError = MySQLdb.IntegrityError
    OperationalError = MySQLdb.OperationalError

try:
    import pymysql
except ImportError:
    logger.warn('ultrasql module not found. please: pip install pymysql')


class PyMySQLConnection(object):
    def __init__(self, host, user, passwd, db, port=3306, autocommit=True, charset='utf8', reconnect_delay=0):
        """
        :param reconnect_delay: 重连等待时间, 0-不重连
        """
        self.args = dict(passwd=passwd, user=user, autocommit=autocommit, charset=charset, database=db)
        if '/' in host:
            self.args['unix_socket'] = host
        else:
            self.args['host'] = host
            self.args['port'] = port
        self.reconnect_delay = reconnect_delay
        self.conn = pymysql.Connection(**self.args)

    def reconnect(self):
        while True:
            try:
                self.conn.close()
            except:
                pass
            try:
                logger.info('trying reconnect..')
                self.conn = pymysql.Connection(**self.args)
                logger.info('reconnected.')
                break
            except:
                logger.error('reconnect except', exc_info=1)
            time.sleep(self.reconnect_delay)

    def execute(self, query, *args, **kwargs):
        """Executes the given query, returning the lastrowid from the query."""
        return self.execute_lastrowid(query, *args, **kwargs)

    def execute_lastrowid(self, query, *args, **kwargs):
        """Executes the given query, returning the lastrowid from the query."""
        try:
            result, cursor = self._execute(query, args, kwargs)
            if result is False:
                return False
            return cursor.lastrowid
        finally:
            if locals().get('cursor'):
                cursor.close()

    def fetchall(self, query, *args, **kwargs):
        """Returns a row list for the given query and args."""
        try:
            result, cursor = self._execute(query, args, kwargs)
            column_names = [d[0] for d in cursor.description]
            if result is False:
                return False
            return [Row(zip(column_names, row)) for row in cursor]
        finally:
            if locals().get('cursor'):
                cursor.close()

    def fetchone(self, query, *args, **kwargs):
        """Returns the (singular) row returned by the given query.
        If the query has no results, returns None.  If it has
        more than one result, raises an exception.
        """
        rows = self.fetchall(query, *args, **kwargs)
        if rows is False:
            return False
        elif not rows:
            return None
        elif len(rows) > 1:
            logger.warn('Multiple rows returned for fetchone')
            return rows[0]
        else:
            return rows[0]

    def executemany(self, query, args):
        """Executes the given query against all the given param sequences.
        We return the lastrowid from the query.
        example:
        executemany('insert into book (name, author) values (%s, %s)',
                    [
                        ('a', u'张三'),
                        ('b', u'李四'),
                        ('c', u'王二')])
        """
        return self.executemany_lastrowid(query, args)

    def executemany_lastrowid(self, query, args):
        """Executes the given query against all the given param sequences.
        We return the lastrowid from the query.
        """
        try:
            result, cursor = self._executemany(query, args)
            if result is False:
                return False
            return cursor.lastrowid
        finally:
            if locals().get('cursor'):
                cursor.close()

    def _execute(self, query, args, kwargs):
        """
        :return: [result, cursor], result: False-表示不重连的时候查询失败(或者是重练成功后又执行失败)
        """
        cursor = self.conn.cursor()
        try:
            logger.debug('sql: %s, args: %s', query, str(args))
            return [cursor.execute(query, args or kwargs), cursor]
        except:
            logger.warn('[Error query]: %s args: %s', query, str(args), exc_info=1)
            if self.reconnect_delay > 0:
                self.reconnect()
                cursor.close()
                try:
                    cursor = self.conn.cursor()
                    return [cursor.execute(query, args or kwargs), cursor]
                except:
                    logger.error('[Error query]:sql: %s args: %s', query, str(args), exc_info=1)
                    cursor.close()
                    return [False, cursor]
            else:
                logger.error('[Error query]:sql: %s args: %s. Not reconnect', query, str(args), exc_info=1)
                try:
                    self.conn.close()
                except:
                    pass
                cursor.close()
                return [False, cursor]

    def _executemany(self, query, args):
        """
        :return: [result, cursor], result: False-表示不重连的时候查询失败(或者是重练成功后又执行失败)
        """
        cursor = self.conn.cursor()
        try:
            logger.debug('sql: %s, args: %s', query, str(args))
            return [cursor.executemany(query, args), cursor]
        except:
            logger.warn('[Error query]: %s args: %s', query, str(args), exc_info=1)
            if self.reconnect_delay > 0:
                self.reconnect()
                cursor.close()
                try:
                    cursor = self.conn.cursor()
                    return [cursor.executemany(query, args), cursor]
                except:
                    logger.error('[Error query]:sql: %s args: %s', query, str(args), exc_info=1)
                    cursor.close()
                    return [False, cursor]
            else:
                logger.error('[Error query]:sql: %s args: %s. Not reconnect', query, str(args), exc_info=1)
                try:
                    self.conn.close()
                except:
                    pass
                cursor.close()
                return [False, cursor]

    def get_fields(self, table_name):
        result, cursor = self._execute('select * from %s limit 0' % table_name, tuple(), tuple())
        if result is False:
            return False
        return [i[0] for i in cursor.description]


try:
    import umysql
except ImportError:
    logger.warn('ultrasql module not found. please: pip install umysql')


class UMySQLConnection(object):
    def __init__(self, host, user, passwd, db, port=3306, autocommit=True, charset='utf8', reconnect_delay=0):
        """
        :param reconnect_delay: 重连等待时间, 0-不重连
        """
        self.args = (host, port, user, passwd, db, autocommit, charset)
        self.reconnect_delay = reconnect_delay
        self.conn = umysql.Connection()
        self.conn.connect(*self.args)

    def reconnect(self):
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
            time.sleep(self.reconnect_delay)

    def query(self, sql, *args, **kwargs):
        """
        :return: False-表示不重连的时候查询失败
        """
        logger.debug('sql: %s args: %s', sql, str(args))
        try:
            return self.conn.query(sql, args)
        except:
            logger.warn('[Error query]: %s', sql, exc_info=1)
            if self.reconnect_delay > 0:
                self.reconnect()
                try:
                    return self.conn.query(sql, args)
                except:
                    logger.error('[Error query]:sql: %s args: %s', sql, str(args), exc_info=1)
                    return False
            else:
                logger.error('[Error query]:sql: %s args: %s. Not reconnect', sql, str(args), exc_info=1)
                return False

    def get_result_rows(self, rs):
        fields = [row[0] for row in rs.fields]
        return [dict(zip(fields, row)) for row in rs.rows]

    def execute(self, sql, *args, **kwargs):
        return self.query(sql, *args, **kwargs)

    def executemany(self, sql, args):
        """警告: 这个和前两个不同,不能提供高性能
        """
        for _args in args:
            ret = self.execute(sql, *_args)
        return ret

    def fetchone(self, sql, *args, **kwargs):
        rs = self.query(sql, *args, **kwargs)
        if rs is False:
            return False
        rows = self.get_result_rows(rs)
        if len(rows) > 0:
            logger.warn('Multiple rows returned for fetchone')
        row = rows and rows[0] or None
        return row

    def fetchall(self, sql, *args, **kwargs):
        rs = self.query(sql, *args, **kwargs)
        if rs is False:
            return False
        rows = self.get_result_rows(rs)
        return rows

    def get_fields(self, table_name):
        result = self.query('select * from %s limit 0' % table_name)
        if result is False:
            return False
        return [i[0] for i in result.fields]


def test_transaction():
    logging.basicConfig(level=logging.DEBUG, format='[%(asctime)-15s %(levelname)s:%(module)s] %(message)s')

    options = dict(host='localhost', user='root', passwd='112358', db='test', reconnect_delay=5)
    # conn = MySQLdbConnection(**options)
    # conn = PyMySQLConnection(**options)
    conn = UMySQLConnection(**options)
    conn.execute('truncate book')
    conn.execute('START TRANSACTION')
    conn.execute('SET AUTOCOMMIT=0')
    conn.execute('insert into book set name="abc", author=%s', u'zhangsan')
    conn.execute('ROLLBACK')
    conn.execute('COMMIT')
    conn.execute('SET AUTOCOMMIT=1')
    print conn.fetchall('select * from book')


def test_client():
    logging.basicConfig(level=logging.DEBUG, format='[%(asctime)-15s %(levelname)s:%(module)s] %(message)s')

    options = dict(host='localhost', user='root', passwd='112358', db='test', reconnect_delay=5)

    mysqldb_conn = MySQLdbConnection(**options)
    print mysqldb_conn.fetchall('select * from book where author=%s', u'大大')
    print mysqldb_conn.get_fields('book')

    umysql_conn = UMySQLConnection(**options)
    print umysql_conn.fetchall('select * from book where author=%s', u'大大')
    print umysql_conn.execute('insert into book set name="abc", author=%s', u'大大')
    print umysql_conn.get_fields('book')

    pymysql_conn = PyMySQLConnection(**options)
    print pymysql_conn.executemany('insert into book (name, author) values (%s, %s)',
                                   [
                                       ('a', u'张三'),
                                       ('b', u'李四'),
                                       ('c', u'王二')])
    print pymysql_conn.get_fields('book')
