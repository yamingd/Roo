# -*- coding: utf-8 -*-
import roo.log
logger = roo.log.logger(__name__)

from session import SessionPlugin
from error import ErrorPlugin
from mail import EmailPlugin

try:
	from amqp import AMQPPlugin
except ImportError:
	logger.info("AMQPPlugin is disabled")
	pass

try:
	from bean import BeanstalkPlugin
except ImportError:
	logger.info("BeanstalkPlugin is disabled")
	pass

try:
    from redis0 import RedisPlugin
except ImportError:
	logger.info("RedisPlugin is disabled")
	pass

try:
    from memcache import MemcachePlugin
except ImportError:
	logger.info("MemcachePlugin is disabled")
	pass

try:
    from mysql import MySQLPlugin
except ImportError:
	logger.info("MySQLPlugin is disabled")
	pass

try:
    from cb import CouchbasePlugin
except ImportError:
	logger.info("CouchbasePlugin is disabled")
	pass

try:
    from imagefs import ImageFSPlugin
except ImportError:
	logger.info("ImageFSPlugin is disabled")
	pass