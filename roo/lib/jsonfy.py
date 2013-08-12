# -*- coding: utf-8 -*-
"""
http://blog.csdn.net/hong201/article/details/3888588
"""
from roo import log
logger = log.logger(__name__)

try:
    import simplejson as _json
except:
    import json as _json

import re
from datetime import datetime


_class_mapped = {}
fmt_dt = u'%Y-%m-%d %H:%M:%S'
re_dt = u'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d+)$'


def _out_dict(obj):
    klass = '%s.%s' % (
        obj.__class__.__module__, obj.__class__.__name__)
    _class_mapped[klass] = obj.__class__
    if isinstance(obj, dict):
        obj['klass'] = klass
        items = obj.items()
    else:
        obj.__dict__['klass'] = klass
        items = obj.__dict__.items()
    for item in items:
        if item[0][0] is '_':
            continue
        if isinstance(item[1], str):
            yield [item[0], item[1].decode()]
        elif isinstance(item[1], datetime):
            yield [item[0], item[1].strftime(fmt_dt + '.%f')]
        else:
            yield item


def str2datetime(s):
    parts = s.split('.')
    dt = datetime.strptime(parts[0], fmt_dt)
    dt = dt.replace(microsecond=int(parts[1]))
    return dt


def _attr_dict(m):
    # logger.debug(str(m.items()))
    for item in m.items():
        v = item[1]
        if isinstance(v, unicode) and re.match(re_dt, v):
            yield [item[0], str2datetime(v)]
        yield item


def _to_klass(m):
    if not hasattr(m, 'get'):
        return m
    klass = m.get('klass', None)
    if not klass:
        return dict(_attr_dict(m))
    klass = _class_mapped.get(klass, None)
    if not klass:
        return dict(_attr_dict(m))
    m = dict(_attr_dict(m))
    return klass(**m)


def dumps(obj):
    if isinstance(obj, unicode) or isinstance(obj, str) or isinstance(obj, long) or isinstance(obj, int) or isinstance(obj, float):
        return _json.dumps(obj)
    if isinstance(obj, datetime):
        return obj.strftime(fmt_dt + '.%f')
    if isinstance(obj, list):
        return _json.dumps(map(dict, map(_out_dict, obj)))
    elif isinstance(obj, dict) or hasattr(obj, '__dict__'):
        return _json.dumps(dict(_out_dict(obj)))


def loads(jstr):
    if not jstr.startswith('{'):
        if isinstance(jstr, unicode) and re.match(re_dt, jstr):
            return str2datetime(jstr)
    m = _json.loads(jstr)
    if isinstance(m, list):
        return list(map(_to_klass, m))
    return _to_klass(m)
