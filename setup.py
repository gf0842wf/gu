#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" description
setup
"""

__author__ = 'wangfei'
__date__ = '2015/03/06'

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='gu',
    version='0.1.0',
    description='gevent net utils',
    author='fk',
    author_email='gf0842wf@gmail.com',
    packages=['gu'],
    package_data={'': ['README.md']},
    license='MIT',
    classifiers=[
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ],
)