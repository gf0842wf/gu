#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" description
mysql db client
兼容 ultramysql, MySQLdb, pymysql
"""

__author__ = 'wangfei'
__date__ = '2015/03/11'

import MySQLdb
import copy
import logging
import time

logger = logging.getLogger(__name__)


class MySQLdbConnection(object):
    """ Need: MySQLdb version >= 1.2.5 and MySQL version > 5.1.12.
    """

    def init(self, host, database, user=None, passwd=None,
             max_idle_time=7 * 3600, connect_timeout=0,
             time_zone="+0:00", charset="utf8", sql_mode="TRADITIONAL",
             **kwargs):

        self.host = host
        self.database = database
        self.max_idle_time = float(max_idle_time)

        args = dict(conv=CONVERSIONS, use_unicode=True, charset=charset,
                    db=database, init_command=('SET time_zone = "%s"' % time_zone),
                    connect_timeout=connect_timeout, sql_mode=sql_mode, **kwargs)
        if user is not None:
            args["user"] = user
        if passwd is not None:
            args["passwd"] = passwd

        # We accept a path to a MySQL socket file or a host(:port) string
        if "/" in host:
            args["unix_socket"] = host
        else:
            self.socket = None
            pair = host.split(":")
            if len(pair) == 2:
                args["host"] = pair[0]
                args["port"] = int(pair[1])
            else:
                args["host"] = host
                args["port"] = 3306

        self._db = None
        self._db_args = args
        self._last_use_time = time.time()
        try:
            self.reconnect()
        except Exception:
            logger.error("Cannot connect to MySQL on %s", self.host,
                         exc_info=True)

    def __init__(self, host, user, passwd, db, port=3306, autocommit=True, charset='utf8', reconnect_delay=0,
                 max_idle_time=7 * 3600, sql_mode='TRADITIONAL', time_zone='+0:00', connect_timeout=0):
        self.host = host
        self.db = db
        self.max_idle_time = float(max_idle_time)

        args = dict(conv=CONVERSIONS, use_unicode=True, charset=charset,
                    db=db, init_command=('SET time_zone = "%s"' % time_zone),
                    connect_timeout=connect_timeout, sql_mode=sql_mode)
        if user is not None:
            args['user'] = user
        if passwd is not None:
            args['passwd'] = passwd

        # We accept a path to a MySQL socket file or a host(:port) string
        if '/' in host:
            args['unix_socket'] = host
        else:
            self.socket = None
            pair = host.split(':')
            if len(pair) == 2:
                args['host'] = pair[0]
                args['port'] = int(pair[1])
            else:
                args['host'] = host
                args['port'] = 3306

        self._db = None
        self._db_args = args
        self._last_use_time = time.time()

        self.autocommit = autocommit
        try:
            self.reconnect()
        except:
            logger.error('Cannot connect to MySQL on %s', self.host, exc_info=1)

    def __del__(self):
        self.close()

    def close(self):
        """Closes this database connection."""
        if getattr(self, "_db", None) is not None:
            self._db.close()
            self._db = None

    def reconnect(self):
        """Closes the existing database connection and re-opens it."""
        self.close()
        self._db = MySQLdb.connect(**self._db_args)
        if self.autocommit:
            self._db.autocommit(True)

    def iter(self, query, *parameters, **kwparameters):
        """Returns an iterator for the given query and parameters."""
        self._ensure_connected()
        cursor = MySQLdb.cursors.SSCursor(self._db)
        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]
            for row in cursor:
                yield Row(zip(column_names, row))
        finally:
            cursor.close()

    def fetchall(self, query, *args, **kwargs):
        """Returns a row list for the given query and parameters."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, args, kwargs)
            column_names = [d[0] for d in cursor.description]
            return [Row(zip(column_names, row)) for row in cursor]
        finally:
            cursor.close()

    def fetchone(self, query, *args, **kwargs):
        """Returns the (singular) row returned by the given query.
        If the query has no results, returns None.  If it has
        more than one result, raises an exception.
        """
        rows = self.query(query, *args, **kwargs)
        if not rows:
            return None
        # elif len(rows) > 1:
        # raise Exception("Multiple rows returned for Database.get() query")
        else:
            return rows[0]

    # rowcount is a more reasonable default return value than lastrowid,
    # but for historical compatibility execute() must return lastrowid.
    def execute(self, query, *args, **kwargs):
        """Executes the given query, returning the lastrowid from the query."""
        return self.execute_lastrowid(query, *args, **kwargs)

    def execute_lastrowid(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the lastrowid from the query."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def execute_rowcount(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the rowcount from the query."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.rowcount
        finally:
            cursor.close()

    def executemany(self, query, parameters):
        """Executes the given query against all the given param sequences.
        We return the lastrowid from the query.
        """
        return self.executemany_lastrowid(query, parameters)

    def executemany_lastrowid(self, query, parameters):
        """Executes the given query against all the given param sequences.
        We return the lastrowid from the query.
        """
        cursor = self._cursor()
        try:
            cursor.executemany(query, parameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def executemany_rowcount(self, query, parameters):
        """Executes the given query against all the given param sequences.
        We return the rowcount from the query.
        """
        cursor = self._cursor()
        try:
            cursor.executemany(query, parameters)
            return cursor.rowcount
        finally:
            cursor.close()

    update = execute_rowcount
    updatemany = executemany_rowcount

    insert = execute_lastrowid
    insertmany = executemany_lastrowid

    def _ensure_connected(self):
        # Mysql by default closes client connections that are idle for
        # 8 hours, but the client library does not report this fact until
        # you try to perform a query and it fails.  Protect against this
        # case by preemptively closing and reopening the connection
        # if it has been idle for too long (7 hours by default).
        if (self._db is None or
                (time.time() - self._last_use_time > self.max_idle_time)):
            self.reconnect()
        self._last_use_time = time.time()

    def _cursor(self):
        self._ensure_connected()
        return self._db.cursor()

    def _execute(self, cursor, query, parameters, kwparameters):
        try:
            return cursor.execute(query, kwparameters or parameters)
        except OperationalError:
            logger.error("Error connecting to MySQL on %s", self.host)
            self.close()
            raise


class Row(dict):
    """A dict that allows for object-like property access syntax."""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)


if MySQLdb is not None:
    # Fix the access conversions to properly recognize unicode/binary
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


class UMySQLConnection(object):
    def __init__(self, host, user, passwd, db, port=3306, autocommit=True, charset='utf8', reconnect_delay=0):
        """
        :param reconnect_delay: 重连等待时间, 0-不重连
        """
        self.args = (host, port, user, passwd, db, autocommit, charset)
        self.reconnect_delay = reconnect_delay
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

