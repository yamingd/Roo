#!/usr/bin/env python
# -*- coding: utf-8 -*-

# -*- coding: utf-8 -*-
import roo.log
logger = roo.log.logger(__name__)

try:
    from roo.cache import CacheBase
    from roo.cache.memcache_client import Memcache
    enabled = True
except Exception, e:
    from roo.cache import CacheBase
    enabled = False
    logger.info('Memcache disabled.')

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
        if enabled and hasattr(application.settings, 'cache'):
            self.conf = application.settings.cache
            self.conf.setdefault('pools', 5)
            self.conf.setdefault('namespace', None)
            self.client = Memcache(
                self.conf.hosts, num_clients=self.conf.pools, namespace=self.conf.namespace)
        else:
            self.client = CacheBase(None)
        setattr(application, 'cache', self.client)

    def on_before(self, controller):
        setattr(controller, 'cache', self.client)
