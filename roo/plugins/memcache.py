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
		self.conf = application.settings.cache
		self.conf.setdefault('pools', 5)
		self.conf.setdefault('namespace', None)
		self.client = Memcache(self.conf.hosts, num_clients=self.conf.pools, namespace=self.conf.namespace)
		setattr(application, 'cache', self.client)

	def on_before(self, controller):
		setattr(controller, 'cache', self.client)
