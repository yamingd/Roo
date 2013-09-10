# -*- coding: utf-8 -*-
import roo.log
logger = roo.log.logger(__name__)

from datetime import datetime

from roo.lib import ODict, jsonfy
from roo.collections import RowSet, RankSet
from roo import encoding


def EntityDef(table, defid):
    """
    @EntityDef('t_person', 10)
    class Person(EntityModel):
        pass
    """
    def wrapper(cls):
        """
        __res_name__ is table_name, __res_id__ is identifier id of that table.
        """
        setattr(cls, '__res_name__', table)
        setattr(cls, '__res_id__', defid)
        return cls
    return wrapper


class NoneResult(object):
    pass


class EntityModel(ODict):

    """
    EntityModel Base class. by defining common functions
    """

    @classmethod
    def init(clz, application):
        setattr(clz, 'app', application)
        setattr(clz, 'models', application.models)

    @classmethod
    def find(clz, id, time=86400, update=False):
        pass

    @classmethod
    def find_one(clz, *args, **kwargs):
        pass

    @classmethod
    def find_list(clz, *args, **kwargs):
        pass

    @classmethod
    def create(clz, *args, **kwargs):
        pass

    @classmethod
    def save(clz, *args, **kwargs):
        pass

    @classmethod
    def remove(clz, *args, **kwargs):
        pass

    @property
    def cacheid(self):
        return getattr(self, '__res_name__') + ':' + getattr(self, 'id')

    def as_json(self):
        return jsonfy.dumps(self)

    @classmethod
    def from_json(clz, json_str):
        m = jsonfy.loads(json_str)
        if isinstance(m, dict):
            return clz(**m)
        return m


def gen_cache_key(clzz_name, args, prefix=''):
    """
    Make the cache key. We have to descend into *a and **kw to make
    sure that only regular strings are used in the key to keep 'foo'
    and u'foo' in an args list from resulting in differing keys
    """
    def _conv(s):
        if isinstance(s, str):
            return s
        elif isinstance(s, unicode):
            return encoding._force_utf8(s)
        elif isinstance(s, datetime):
            return s.strftime('%Y%m%d')
        else:
            return str(s)
    key = clzz_name.lower()
    if prefix:
        key = key + ':' + prefix
    if args:
        key = key + ':' + ':'.join([_conv(x) for x in args if x])
    return key


def prstat(model_clzz_name):
    """
    @prstat('UserStat')
    def stat(self): pass
    """
    def fwrapper(func):
        def wrapper(*args, **kwargs):
            _self = args[0]
            if not hasattr(_self, '_stat'):
                _clzz = _self.__class__
                #print _self, _clzz
                _clzz = getattr(_clzz.models, model_clzz_name)
                _self._stat = _clzz.find(getattr(_self, 'id'))
            return _self._stat
        return wrapper
    return fwrapper


def prref(model_clzz_name, prop_name, idtype=long):
    """
    @prref('User', 'user_id'):
    def user(self): pass
    """
    def fwrapper(func):
        def wrapper(*args, **kwargs):
            _self = args[0]
            _name = '_o_' + prop_name
            if not hasattr(_self, _name):
                _clzz = _self.__class__
                _clzz = getattr(_clzz.models, model_clzz_name)
                val = getattr(_self, prop_name)
                if val is None:
                    logger.error('prref: value is None. p=' + prop_name)
                    return None
                if idtype:
                    val = idtype(val)
                if not val or val <= 0:
                    return _clzz()
                _ob = _clzz.find(val)
                setattr(_self, _name, _ob)
            return getattr(_self, _name)
        return wrapper
    return fwrapper


def lrange(model_clazz_name, lrange_key):
    """
    @lrange('UserAttachment', 'user:attachs:%s')
    def attachments(self, index=1, size=10): pass
    """
    def fwrapper(func):
        def wrapper(*args, **kwargs):
            _self = args[0]
            pi = kwargs.get('index', 1)
            ps = kwargs.get('size', 10)
            _name = '_lr_' + model_clazz_name.lower()
            if not hasattr(_self, _name):
                _clzz = _self.__class__
                _clzz = getattr(_clzz.models, model_clazz_name)
                r = _clzz.app.redis
                key = lrange_key % _self.id
                start = (pi - 1) * ps
                items = r.lrange(key, start, start + ps - 1)
                total = r.llen(key)
                result = RowSet(items, _clzz, total=total, limit=ps, start=pi)
                setattr(_self, _name, result)
            return getattr(_self, _name)
        return wrapper
    return fwrapper


def zrange(model_clazz_name, field_name, zrange_key):
    """
    @zrange('UserAttachment', 'field', 'user:attachs:%s')
    def attachments(self, index=1, size=10): pass
    """
    def fwrapper(func):
        def wrapper(*args, **kwargs):
            _self = args[0]
            pi = kwargs.get('index', 1)
            ps = kwargs.get('size', 10)
            _name = '_zr_' + field_name
            if not hasattr(_self, _name):
                _clzz = _self.__class__
                _clzz = getattr(_clzz.models, model_clazz_name)
                r = _clzz.app.redis
                start = (pi - 1) * ps
                key = zrange_key % _self.id
                items = r.zrange(
                    key, start, start + ps - 1, desc=False, withscores=True)
                result = RankSet(
                    items, _clzz, field_name, limit=ps, start=pi)
                setattr(_self, _name, result)
            return getattr(_self, _name)
        return wrapper
    return fwrapper
