"""
Tests for executor module
"""
# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring
import os.path
import logging
from tornado.concurrent import Future
from tornado.testing import AsyncHTTPTestCase, gen_test
from tornado.web import RequestHandler, Application
from tornado.httpclient import HTTPError

logger = logging.getLogger(__name__)

