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
from datetime import date, time

fmt_dt = u'%Y-%m-%d %H:%M:%S'
fmt_dtss = u'%Y-%m-%d %H:%M:%S.%f'
re_dt = u'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}.\d+)$'


class CJsonEncoder(_json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime(fmt_dtss)
        elif isinstance(obj, date):
            return obj.strftime('%Y-%m-%d')
        elif isinstance(obj, time):
            return obj.strftime('%H:%M:%S')
        else:
            return _json.JSONEncoder.default(self, obj)


class CJsonDecoder(_json.JSONDecoder):
    def decode(self, json_string):
        json_data = _json.loads(json_string)
        if not hasattr(json_data, 'keys'):
            return json_data
        for key in json_data.keys():
            val = json_data[key]
            fval = isinstance(val, unicode) or isinstance(val, str)
            if fval and re.match(re_dt, val):
                try:
                    json_data[key] = str2datetime(val)
                except TypeError:
                    # It's not a datetime/time object
                    pass
        return json_data


def str2datetime(s):
    parts = s.split('.')
    dt = datetime.strptime(parts[0], fmt_dt)
    dt = dt.replace(microsecond=int(parts[1]))
    return dt


def dumps(obj):
    return _json.dumps(obj, cls=CJsonEncoder)


def loads(jstr):
    return _json.loads(jstr, cls=CJsonDecoder)

if __name__ == '__main__':
    txt = '{"qq": "", "doc_type": "user", "client_ip": "192.168.1.105", "name": "elsadmin", "roles": ["Administrator"], "level": "L0", "mobile": "", "last_loginat": "", "create_at": "2013-08-24 23:52:43.498737", "real_name": "Administrator", "klass": "app.models.user.User", "client_id": "", "hash_passwd": "e73d094af5cc98447eb6e545e721e4be0b5c4f2b", "wechat_id": "", "reg_app_id": "", "id": 1}'
    print loads(txt)
