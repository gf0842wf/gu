#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" description
延时,循环,超时等gevent工具
"""

import gevent
import time
import logging

logger = logging.getLogger(__name__)


class Timeout(gevent.Timeout):
    def __init__(self, seconds=None, exception=None):
        gevent.Timeout.__init__(self, seconds, exception)
        self.start_time = None

    def start(self):
        self.start_time = time.time()
        gevent.Timeout.start(self)

    @property
    def passed(self):
        if self.stime is None:
            return 0
        now = time.time()
        return now - self.stime

    @property
    def rest(self):
        if self.stime is None: return 0
        if self.seconds is None: return 0
        now = time.time()
        return self.seconds - (now - self.stime)