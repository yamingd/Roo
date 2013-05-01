# -*- coding: utf-8 -*-
import roo.log
logger = roo.log.logger(__name__)


from roo.cache.memcache_client import Memcache
from roo.plugin import BasePlugin, plugin


@plugin
class MemcachePlugin(BasePlugin):
	"""
	config options as:
	cache.hosts = ['127.0.0.1:11211']
	cache.pools = 5
	cache.namespace = ''
	"""
	name = 'cache'
	
	def __init__(self, application):
		BasePlugin.__init__(self, application)
		self.application = application
		conf = application.settings.cache
		if 'pools' not in conf:
			conf.pools = 5
		if 'namespace' not in conf:
			conf.namespace = None
		self.conf = conf
		self.client = Memcache(self.conf.hosts, num_clients=self.conf.pools, namespace=self.conf.namespace)
		setattr(application, 'cache', self.client)

	def on_before(self, controller):
		setattr(controller, 'cache', self.client)
