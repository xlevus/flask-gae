#!/usr/bin/env python

import os
from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='flask-gae',
    version='0.0.1',
    description='A collection of utilities for using Flask on AppEngine.',
    long_description=read('README.md'),
    author='Chris Targett',
    author_email='chris@xlevus.net',
    url='http://github.com/xlevus/flask-gae',
    packages=['flask_gae'],
    install_requires=['flask'],
    classifiers=[
    ],
    keywords='flask wtforms appengine ndb',
    license='',
    test_suite='nose.collector',
    tests_require=['nose', 'flask-testing', 'mock'],
)
