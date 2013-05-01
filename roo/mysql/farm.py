# -*- coding: utf-8 -*-
import random
from roo import threadlocal
from client import ConnectionProxy

"""
shards = {
    "shard01" : ("localhost:33690", 1, 512),
    "shard02" : ("localhost:33691", 513, 1024),
 }
"""


class IdService(object):

    def __init__(self, application):
        self.app = application

    def get(self, object_type_id):
        """
        return full_id
        """
        pass

    def parse(self, full_id):
        """
        return (shard_id, local_id)
        """


class RedisIdService(IdService):

    def get(self, object_type_id):
        r = self.app.redis
        rkey = u'pk:o%s' % object_type_id
        sid = r.incr(rkey)
        return sid

    def parse(self, full_id):
        return (1, full_id)


class ShardIdService(IdService):
    SHARD_LEN = 46
    OBJECT_LEN = 10

    def uuid(self, shard_id, object_type, local_id):
        sid = shard_id << self.SHARD_LEN | object_type << self.OBJECT_LEN | local_id
        return sid

    def get(self, object_type_id, shard_id):
        rkey = u'pk:o%s' % object_type_id
        r = self.app.redis
        local_id = r.incr(rkey)
        if not shard_id:
            return None
        full_id = self.uuid(shard_id, object_type_id, local_id)
        return full_id

    def parse(self, full_id):
        shard_id = full_id >> self.SHARD_LEN
        temp = full_id - (shard_id << self.SHARD_LEN)
        object_type = temp >> self.OBJECT_LEN
        local_id = temp - (object_type << self.OBJECT_LEN)
        return (shard_id, local_id)


class MySQLFarmConnectionProxy(ConnectionProxy):
    pass


class MySQLFarm(object):

    def __init__(self, application, id_service=None):
        self.servers = {}
        self.cons = {}
        self.app = application
        self.conf = application.settings.mysql
        self.db0 = None
        self.id_service = ShardIdService(
            application) if id_service is None else id_service
        self.initialize()

    def dbname(self, index):
        return "%s%05d" % (self.conf.name, index)

    def initialize(self):
        """
        cons = {'host':connection}
        servers = {'shardId':'host'}
        """
        for k in self.conf.servers:
            host, start, end = self.conf.servers[k]
            con = None
            for index in xrange(start, end + 1):
                database = self.dbname(index)
                if con is None:
                    con = MySQLFarmConnectionProxy(
                        host=host, database=database, num_clients=self.conf.num_clients,
                        user=self.conf.user, passwd=self.conf.passwd, debug=self.conf.debug)
                    self.cons[host] = con
                self.servers[database] = host
                if not self.db0:
                    self.db0 = con
        if len(self.servers) > 1:
            self.db0 = None

    def select(self, object_type, shard_id=None):
        """
        find a shard database
        return (shard_id, db)
        """
        if self.db0:
            return (0, self.db0)
        if not shard_id:
            count = len(self.servers)
            shard_id = random.randint(1, count)
        db1 = self.servers.get(self.dbname(shard_id))
        return (shard_id, db1)

    def random(self):
        """
        return shardid
        """
        if self.db0:
            return None
        count = len(self.servers)
        shard_id = random.randint(1, count)
        return shard_id

    def get(self, shard_id):
        """
        return db
        """
        if self.db0:
            return self.db0
        host = self.servers.get(self.dbname(shard_id))
        db1 = self.cons[host]
        return db1

    def find(self, full_id):
        """
        parse full_id to locate shard database
        return (shard_id, local_id, db)
        """
        if self.db0:
            return (0, full_id, self.db0)
        shard_id, object_type, local_id = self.id_service.parse(full_id)
        host = self.servers.get(self.dbname(shard_id))
        db1 = self.cons[host]
        return (shard_id, local_id, db1)

    def relocate(self, shard_id, host):
        database = self.dbname(shard_id)
        c = MySQLFarmConnectionProxy(
            host=host, database=database, num_clients=10,
            user=self.conf.user, passwd=self.conf.passwd, debug=self.conf.debug)
        self.cons[host] = c
        self.servers[database] = host

    def genid(self, object_type_id):
        shardid = threadlocal.get('dbshardid')
        return self.id_service.get(object_type_id, shardid)

    def setdb(self, shardid):
        threadlocal.set('dbshardid', shardid)
        threadlocal.set('dbname', self.dbname(shardid))

    @property
    def current(self):
        return (threadlocal.get('dbshardid'), threadlocal.get('dbname'))
