# -*- coding: utf-8 -*-
import roo.log
logger = roo.log.logger(__name__)

import redis

from roo import datefy
from roo.plugin import BasePlugin, plugin
from roo.model import EntityModel


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


class RedisBaseModel(EntityModel):
    redis_key = '%s:%s'

    @classmethod
    def init(clz, application):
        EntityModel.init(application)
        setattr(clz, 'redis', application.redis)

    @classmethod
    def incr(clz, *args):
        """
        更新操作统计数字
        incr(id, 'a', 1, 'b', 2, 'c', 3)
        """
        id = args[0]
        args = args[1:]
        r = clz.redis
        rkey = clz.redis_key % (clz.__name__.lower(), id)
        for k in xrange(len(args) / 2):
            r.hincrby(rkey, args[k], int(args[k + 1]))
        rkey = clz.redis_key % (
            clz.__name__.lower() + ':ts', datefy.today_str())
        r.sadd(rkey, id)

    def incrby(self, *args):
        clz = self.__class__
        args = list(args)
        args.insert(0, self.id)
        clz.incr(*args)

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            return 0

    @classmethod
    def find(clz, id, time=86400, update=False):
        """
        读取用户操作统计数字
        """
        r = clz.redis
        rkey = clz.redis_key % (clz.__name__.lower(), id)
        stats = r.hgetall(rkey)
        if stats is None:
            stats = {}
        m = {'id': id}
        for k in stats:
            m[k] = int(stats[k])
        return clz(m)
