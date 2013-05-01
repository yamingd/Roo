# -*- coding: utf-8 -*-
import roo.log
logger = roo.log.logger(__name__)

import string
from datetime import datetime

from roo import threadlocal, datefy
from roo.plugin import BasePlugin, plugin
from roo.mysql import MySQLFarm
from roo.model import EntityModel, gen_cache_key, NoneResult


@plugin
class MySQLPlugin(BasePlugin):

    """
    config options as :
    mysql.user = 'user'
    mysql.passwd = 'secret'
    mysql.num_clients = 5
    mysql.servers = {
            "shard01" : ("localhost:33690", 1, 512),
            "shard02" : ("localhost:33691", 513, 1024),
    }
    mysql.debug = False
    """
    name = "mysql"

    def __init__(self, application):
        BasePlugin.__init__(self, application)
        self.app = application
        conf = application.settings.mysql
        conf.setdefault('num_clients', 5)
        conf.setdefault('debug', False)
        self.conf = conf
        self.farm = MySQLFarm(application)
        setattr(application, 'mysql', self.farm)

    def on_before(self, controller):
        setattr(controller, 'mysql', self.farm)
        # locate db-connection if sharded
        user = threadlocal.get_user()
        if user:
            shardid, _ = self.farm.find(long(user.id))
            self.farm.setdb(shardid)
        else:
            shardid = self.farm.random()
            self.farm.setdb(shardid)

        if self.debug:
            logger.debug("dbshardid:%s, dbname:%s" % self.farm.current)


class MySQLModel(EntityModel):

    @classmethod
    def dbm(clz):
        return clz.app().mysql

    @classmethod
    def dbsess(clz):
        shardid, _ = clz.dbm().current
        setattr(clz, 'shardid', shardid)
        return clz.dbm().get(shardid)

    @classmethod
    def mc(clz):
        return clz.app().cache

    @classmethod
    def find(clz, id, time=86400, update=False):
        sql = u'select * from %s where id = %s' % (clz.__res_name__, '%s')
        cache_key = gen_cache_key(clz.__name__, [id])
        cache = clz.mc()
        obj = cache.get(cache_key) if not update else None
        if obj is None:
            obj = clz.dbsess().get(sql, id, clz=clz)
            if obj is not None:
                cache.set(cache_key, obj, time=time)
            else:
                cache.set(cache_key, NoneResult(), time=time)
                return None
        if not isinstance(obj, NoneResult):
            return obj
        return None

    @classmethod
    def find_one(clz, *args, **kwargs):
        """
        args: (sql, ids)
        kwargs : {'update':True, 'time' : 10, 'prefix':None}
        """
        prefix = kwargs.get('prefix', 'unique')
        _from_db = kwargs.get('update', False)
        _time = kwargs.get('time', 60)
        query = args[0]
        args = args[1:]
        cache_key = gen_cache_key(clz.__name__, *args, prefix=prefix)
        cache = clz.mc()
        obj = cache.get(cache_key) if not _from_db else None
        if obj is None:
            obj = clz.dbsess().get(query, *args, clz=clz)
            if obj is not None:
                cache.set(cache_key, obj, time=_time)
            else:
                cache.set(cache_key, NoneResult(), time=_time)
                return None
        if not isinstance(obj, NoneResult):
            return obj
        return None

    @classmethod
    def find_list(clz, *args, **kwargs):
        """
        ident = [shard-uuid, id2, id3], shard-uuid used to locate db
        """
        prefix = kwargs.get('prefix', None)
        _from_db = kwargs.get('update', False)
        start = kwargs.get('start', None)
        limit = kwargs.get('limit', None)
        _time = kwargs.get('time', 600)
        if start and limit:
            start = int(start)
            offset = (start - 1) * limit
            args.append(offset)
            args.append(limit)
        query = args[0]
        args = args[1:]
        cache_key = gen_cache_key(clz.__name__, args, prefix=prefix)
        cache = clz.mc()
        sets = cache.get(cache_key) if not _from_db else None
        if sets is None:
            sets = clz.dbsess().query(query, *args, clz=clz)
            if sets:
                cache.set(cache_key, sets, _time)
            else:
                cache.set(cache_key, NoneResult(), 60)
                return []
        if not isinstance(sets, NoneResult):
            return sets
        return []

    @classmethod
    def create(clz, *args, **kwargs):
        """
        args[0] = sql,
        args[1:] = datas
        """
        sql = args[0]
        params = list(args)
        db0 = clz.dbsess()
        full_id = clz.dbm().genid(clz.__res_id__)  # id generate
        params[0] = full_id
        db0.execute(sql, *params)
        logger.debug("create record at server: shard_id=%s, full_id=%s" %
                     (clz.shardid, full_id))
        return full_id

    @classmethod
    def save(clz, *args, **kwargs):
        """
        args = (sql, param1, param2, ..., id)
        update %s set %s = %s where id = %s
        """
        sql = args[0]
        args = args[1:]
        db0 = clz.dbsess()
        db0.execute(sql, *args)
        update = kwargs.get('update', True)
        if update:
            time = kwargs.get('time', 10)
            cache = clz.mc()
            cache_key = gen_cache_key(clz.__name__, [args[-1]])
            cache.set(cache_key, NoneResult(), time=time)

    @classmethod
    def remove(clz, *args, **kwargs):
        time = kwargs.get('time', 10)
        sql = u'update %s set if_deleted=1, delete_at=%s where id = %s' % (
            clz.__res_name__, '%s', '%s')
        vargs = [datetime.now(), args[0]]
        clz.dbsess().execute(sql, *vargs)
        cache_key = gen_cache_key(clz.__name__, args)
        cache = clz.mc()
        cache.set(cache_key, NoneResult(), time=time)


class MySQLStatModel(MySQLModel):
    stat_key = '%s:%s'

    @classmethod
    def find(clz, id, time=86400, update=False):
        """
        读取用户操作统计数字
        """
        r = clz.app().redis
        rkey = clz.stat_key % (clz.__name__.lower(), id)
        stats = r.hgetall(rkey)
        if stats is None:
            stats = {}
        m = {'id': id}
        for k in stats:
            m[k] = int(stats[k])
        return clz(m)

    @classmethod
    def incr(clz, *args):
        """
        更新操作统计数字
        incr(id, 'a', 1, 'b', 2, 'c', 3)
        """
        id = args[0]
        args = args[1:]
        r = clz.app().redis
        rkey = clz.stat_key % (clz.__name__.lower(), id)
        for k in xrange(len(args) / 2):
            r.hincrby(rkey, args[k], int(args[k + 1]))
        rkey = clz.stat_key % (
            clz.__name__.lower() + ':ts', datefy.today_str())
        r.sadd(rkey, id)

    def incrby(self, *args):
        clz = self.__class__
        args = list(args)
        args.insert(0, self.id)
        clz.incr(*args)

    @classmethod
    def sync_todb(clz, date=None):
        """
        同步内存更新回数据库.
        """
        r = clz.app().redis
        date = date or datefy.yesterday()
        tsrkey = clz.stat_key % (
            clz.__name__.lower() + ':ts', datefy.format(date))
        rids = r.smembers(tsrkey)
        if rids is None:
            return
        for aid in rids:
            rkey = clz.stat_key % (clz.__name__.lower(), aid)
            stats = r.hgetall(rkey)
            if stats is None:
                continue
            params = []
            sql = " select id from %s where id = %s" % (clz.__res_name__, "%s")
            temp = clz.find_one(sql, aid)
            if temp is None:
                fields = stats.keys()
                params.append(aid)
                for k in fields:
                    params.append(stats[k])
                sql = " insert into %s(id, %s)values(%s, %s)" % (
                    clz.__res_name__, ', '.join(fields), '%s', '%s' * len(fields))
            else:
                sql = " update %s set " % clz.__res_name__
                sets = []
                for k in stats:
                    sets.append(" %s = %s " % (k, '%s'))
                    params.append(int(stats[k]))
                sql = sql + string.join(sets, ', ') + ' where id = %s'
                params.append(aid)
            clz.dbsess().execute(sql, *params)
        r.delete(tsrkey)

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            return 0
