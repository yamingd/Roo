# -*- coding: utf-8 -*-
import roo.log
logger = roo.log.logger(__name__)

from couchbase import Couchbase
from roo.plugin import BasePlugin, plugin
from roo.model import EntityModel
from roo.collections import RowSet, StatCollection


@plugin
class CouchbasePlugin(BasePlugin):

    """
    config options as follow:
    couchbase.ip : "127.0.0.1"
    couchbase.port : 8091
    couchbase.user : "admin"
    couchbase.passwd : "secret"
    couchbase.bucket : "default"
    """
    name = 'couchbase'

    def __init__(self, application):
        BasePlugin.__init__(self, application)
        self.conf = application.settings.couchbase
        self.conf.setdefault('port', 8091)
        self.client = Couchbase('%s:%s' % (
            self.conf.ip, self.conf.port), username=self.conf.user, password=self.conf.passwd)
        setattr(application, 'cb', self.client)
        setattr(application, 'cq', CouchQuery)

    def on_before(self, controller):
        setattr(controller, 'cb', self.client)
        setattr(controller, 'cq', CouchQuery)


class CouchbaseModel(EntityModel):
    bucket_name = None
    ddoc_name = None

    @classmethod
    def init(clz, application):
        EntityModel.init(application)
        if clz.bucket_name is None:
            clz.bucket_name = application.settings.couchbase.bucket
        setattr(clz, 'bucket', application.cb[clz.bucket_name])
        ddocs = clz._ddoc_views()
        if ddocs:
            clz.bucket[clz.get_ddoc_name()] = ddocs

    @classmethod
    def pkid(clz):
        key = u'pk:o%s' % clz.__res_name__
        return clz.bucket.incr(key, init=1)[0]

    @classmethod
    def get_ddoc(clz):
        return clz.bucket[clz.get_ddoc_name()]

    @classmethod
    def get_ddoc_name(clz):
        if clz.ddoc_name:
            ddoc_name = clz.ddoc_name
        else:
            ddoc_name = clz.__res_name__
        return ddoc_name
    
    @classmethod
    def query(clz):
        return CouchQuery()

    @classmethod
    def find(clz, id, time=86400, update=False):
        key = u'%s:%s' % (clz.__res_name__, id.__str__())
        json_str = clz.bucket.get(key)
        return clz.from_json(json_str)

    @classmethod
    def find_one(clz, *args, **kwargs):
        """
        args: (view_name, ddoc_name)
        kwargs : {'key1':'value1','key2':'value2'}
        """
        kwargs.set('limit', 1)
        rs = clz.find_list(*args, **kwargs)
        return rs.pop()

    @classmethod
    def find_list(clz, *args, **kwargs):
        """
        args: (view_name, ddoc_name)
        kwargs : {
            'limit':'20','page':'value2', 'descending': true|false,
            "startkey": ABC, "endkey": DEF,
            "key": ABC, "keys": []
            }
        results:
        {
          "total_rows": 576,
          "rows" : [
              {"value" : 13000, "id" : "James", "key" : ["James", "Paris"] },
              {"value" : 20000, "id" : "James", "key" : ["James", "Tokyo"] },
              {"value" : 5000,  "id" : "James", "key" : ["James", "Paris"] },
            ]
        }
        link:
        http://www.couchbase.com/docs/couchbase-manual-2.0/couchbase-views-writing-querying-selection.html
        """
        kwargs.setdefault('limit', 20)
        kwargs.setdefault('page', 1)

        limit = kwargs.get('limit', 20)
        page = kwargs.get('page', 1)
        start = (page - 1) * limit
        kwargs['skip'] = start
        if limit <= 0:
            kwargs.pop('limit', None)
            kwargs.pop('skip', None)
        kwargs.pop('page', None)

        if len(args) >= 2:
            ddoc = clz.bucket['_design/' + args[1]]
        else:
            ddoc = clz.get_ddoc()
        results = ddoc[args[0]].results(params=kwargs)
        total = results.total_rows
        itemids = []
        for item in results:
            itemids.append(item['id'])
        rs = RowSet(itemids, clz, total=total, limit=limit, start=start)
        return rs

    @classmethod
    def find_stats(clz, *args, **kwargs):
        """
        args: (view_name, ddoc_name)
        kwargs : {
            'limit':'20','page':'value2', 'descending': true|false,
            "startkey": ABC, "endkey": DEF,
            "key": ABC, "keys": [],
            "group_level": 3
            }
        results:
        {"rows":[
            {"key":[2010,7,22],"value":{"beer":5825,"brewery":1385}},
            {"key":[2010,7,29],"value":{"beer":1}},
            {"key":[2010,10,24],"value":{"brewery":1}},
            {"key":[2010,11,8],"value":{"beer":1,"brewery":1}},
            ]
        }
        link:
        http://www.couchbase.com/docs/couchbase-manual-2.0/couchbase-views-writing-querying-selection.html
        """
        kwargs.setdefault('limit', 20)
        kwargs.setdefault('page', 1)

        limit = kwargs.get('limit', 20)
        page = kwargs.get('page', 1)
        start = (page - 1) * limit
        kwargs['skip'] = start
        if limit <= 0:
            kwargs.pop('limit', None)
            kwargs.pop('skip', None)
        kwargs.pop('page', None)

        if len(args) >= 2:
            ddoc = clz.bucket['_design/' + args[1]]
        else:
            ddoc = clz.get_ddoc()
        kwargs['group'] = True
        kwargs['reduce'] = True
        results = ddoc[args[0]].results(params=kwargs)
        return StatCollection(results)

    @classmethod
    def get_stat(clz, *args, **kwargs):
        kwargs['limit'] = 1
        rs = clz.find_stats(*args, **kwargs)
        if rs.total_rows > 0:
            return rs[0]
        return None

    @classmethod
    def create(clz, *args, **kwargs):
        """
        args[0] = clz(),
        kwargs = {'time':86000 or None}
        """
        o = args[0]
        if not o.id:
            o.id = clz.pkid()
        o.doc_type = clz.__res_name__
        if not kwargs:
            kwargs = {}
        exp = kwargs.get('time', 0)
        flags = kwargs.get('flags', 0)
        key = '%s:%s' % (clz.__res_name__, o.id)
        clz.bucket.add(key, exp, flags, o.as_json())
        return o.id

    @classmethod
    def save(clz, *args, **kwargs):
        """
        args[0] = clz(),
        kwargs = {'time':86000 or None}
        """
        o = args[0]
        if not kwargs:
            kwargs = {}
        exp = kwargs.get('time', 0)
        flags = kwargs.get('flags', 0)
        key = '%s:%s' % (clz.__res_name__, o.id)
        clz.bucket.set(key, exp, flags, o.as_json())
        return True

    @classmethod
    def remove(clz, *args, **kwargs):
        """
        args[0] = id
        """
        key = u'%s:%s' % (clz.__res_name__, args[0])
        clz.bucket.delete(key)

    @classmethod
    def _ddoc_views(clz):
        return None


def date_to_array(date):
    """
    return [year, month, day, hour, minute, second]
    """
    if date is None:
        return
    return [date.year, date.month, date.day, date.hour, date.minute, date.second]


class CouchQuery(object):

    def __init__(self):
        self.q = {'limit': 20, 'page': 1}

    def limit(self, count):
        self.q['limit'] = count
        return self

    def page(self, index):
        count = self.q.get('limit', 0) * (index - 1)
        self.q['skip'] = count
        return self

    def key(self, value):
        self.q['key'] = value
        return self

    def keys(self, *args):
        self.q['keys'] = list(args)
        return self

    def startkey(self, value):
        self.q['startkey'] = value
        return self

    def endkey(self, value):
        self.q['endkey'] = value
        return self

    def startdate(self, value):
        return self

    def enddate(self, value):
        return self

    def group(self, value=True):
        if value:
            self.q['group'] = True
            self.q['reduce'] = True
        else:
            self.q.pop('group', None)
            self.q.pop('reduce', None)
        return self

    def group_level(self, level):
        self.q['group'] = True
        self.q['group_level'] = level
        self.q['reduce'] = True
        return self

    def reduce(self, value=True):
        if value:
            self.q['reduce'] = True
        else:
            self.q.pop('reduce', None)
        return self

    def sort(self, descending=True):
        self.q['descending'] = descending
        return self
