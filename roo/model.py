#!/usr/bin/env python
# -*- coding: utf-8 -*-

import roo.log
logger = roo.log.logger(__name__)

from datetime import datetime
from pprint import pformat
from UserDict import DictMixin
from cStringIO import StringIO

from roo.lib import jsonfy
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


class Field(dict):
 
    """
    Container of field metadata
    """
    def __init__(self, default=None):
        self.default = default

    def parse(self, value):
        return value


class IntField(Field):
    pass


class LongField(Field):
    pass


class BooleanField(Field):
    pass


class FloatField(Field):
    pass


class DateField(Field):
    pass


class DateTimeField(Field):
    pass


class TimestampField(Field):
    pass


class TextField(Field):
    pass


class StringField(Field):
    pass


class IPField(Field):
    pass


def ip_int(x):
    if isinstance(x, str) or isinstance(x, unicode):
        iv = sum([256**j*int(i) for j,i in enumerate(x.split('.')[::-1])])
        return iv
    else:
        s = '.'.join([str(x/(256**i)%256) for i in range(3,-1,-1)])
        return s


class Sqlb(object):
    def __init__(self):
        self.f = StringIO()
    
    def append(self, s):
        self.f.write(s)
        return self

    @property
    def s(self):
        return self.f.getvalue()
    
    def __del__(self):
        self.f.close()
        self.f = None


class ModelMeta(type):
 
    def __new__(mcs, class_name, bases, attrs):
        #print mcs
        #print class_name
        #print bases
        #print attrs
        fields = {}
        new_attrs = {}
        for n, v in attrs.iteritems():
            if isinstance(v, Field):
                fields[n] = v
            else:
                new_attrs[n] = v
 
        cls = type.__new__(mcs, class_name, bases, new_attrs)
        cls.__fields__ = cls.__fields__.copy()
        for b in bases:
            if hasattr(b, '__fields__'):
                fields.update(b.__fields__)
        cls.__fields__.update(fields)
        cls._sql_fields_ = ','.join(fields.keys())
        cls._sql_cols_ = cls._sql_fields_.split(',')
        
        #logger.info(bases)
        #logger.info(class_name)
        #logger.info(cls._sql_fields_)
        #logger.info(cls._sql_cols_)

        return cls


class DictItem(DictMixin, object):
 
    __fields__ = {}
 
    def __init__(self, *args, **kwargs):
        self._values = {}
        if args or kwargs:  # avoid creating dict for most common case
            for k, v in dict(*args, **kwargs).iteritems():
                self[k] = v
 
    def __getitem__(self, key):
        return self._values[key]
 
    def __setitem__(self, key, value):
        if key in self.__fields__:
            self._values[key] = self.__fields__[key].parse(value)
        else:
            self._values[key] = value
            logger.warn("%s does not support field: %s" % (self.__class__.__name__, key))
 
    def __delitem__(self, key):
        del self._values[key]
 
    def __getattr__(self, name):
        if name not in self._values:
            raise AttributeError(name)
        return self._values[name]
 
    def __setattr__(self, name, value):
        if name.startswith('_'):
            super(DictItem, self).__setattr__(name, value)
            return
        if name in self.__fields__:
            self._values[name] = self.__fields__[name].parse(value)
        else:
            self._values[name] = value
            logger.warn("%s does not support field: %s" % (self.__class__.__name__, name))

    def keys(self):
        return self._values.keys()
 
    def __repr__(self):
        return pformat(dict(self))
 
    def copy(self):
        return self.__class__(self)
    
    def as_json(self):
        return jsonfy.dumps(self._values)

    @classmethod
    def from_json(clz, json_str):
        m = jsonfy.loads(json_str)
        if isinstance(m, dict):
            return clz(**m)
        return m


class ModelDef(DictItem):
 
    """
    model definition of columns, fields
    """
    __metaclass__ = ModelMeta


class ModelMixin(object):

    """
    model handle methods. such as db, cache
    """

    @classmethod
    def init(clz, application):
        setattr(clz, 'app', application)
        setattr(clz, 'models', application.models)

    @classmethod
    def find(clz, id, time=86400, update=False, callback=None):
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
    
    @classmethod
    def map(clz, fields, values):
        if values is None:
            return None
        if len(values) == 0:
            return []
        rs = []
        for rec in values:
            args = {}
            for f, v in zip(fields, rec):
                args[f] = v
            rs.append(clz(**args))
        return rs


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

