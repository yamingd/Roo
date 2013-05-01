# -*- coding: utf-8 -*-
import roo.log
logger = roo.log.logger(__name__)

import redis

from roo.plugin import BasePlugin, plugin


@plugin
class RedisPlugin(BasePlugin):
	"""
	config options as :
	redis.host = '127.0.0.1'
	redis.port = 6379
	redis.namespace = ''
	"""
	name = "redis"

	def __init__(self, application):
		BasePlugin.__init__(self, application)
		self.application = application
		conf = application.settings.redis
		if 'port' not in conf:
			conf.port = 6379
		if 'namespace' not in conf:
			conf.namespace = None
		if 'host' not in conf:
			conf.host = '127.0.0.1'
		self.conf = conf
		self.client = redis.StrictRedis(host=conf.host, port=conf.port)
		setattr(application, 'redis', self.client)

	def on_before(self, controller):
		setattr(controller, 'redis', self.client)

	def connect(self):
		c = redis.StrictRedis(host=self.conf.host, port=self.conf.port)
		return c
