# -*- coding: utf-8 -*-

from session import SessionPlugin
from error import ErrorPlugin
from mail import EmailPlugin

try:
	from amqp import AMQPPlugin
except ImportError:
	pass

try:
	from bean import BeanstalkPlugin
except ImportError:
	pass


from redis0 import RedisPlugin
from memcache import MemcachePlugin

from mysql import MySQLPlugin

try:
    from cb import CouchbasePlugin
except ImportError:
	pass
