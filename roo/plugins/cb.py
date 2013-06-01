# -*- coding: utf-8 -*-
import roo.log
logger = roo.log.logger(__name__)

import os
from datetime import datetime
from couchbase import Couchbase
from roo.plugin import BasePlugin, plugin
from roo.model import EntityModel
from roo.collections import RowSet
from roo.controller import Controller
from roo.router import route


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
        self.buckets = {}
        self.conf = application.settings.couchbase
        self.conf.setdefault('port', 8091)
        setattr(application, 'cbb', self.get_bucket)
        setattr(application, 'cq', CouchQuery)
        self.scan_ddoc(os.path.join(application.root, 'ddoc'))
        logger.info('ddocs:%s, views:%s' % (len(self.ddocs), len(self.views)))

    def on_before(self, controller):
        setattr(controller, 'cq', CouchQuery)

    def get_bucket(self, name):
        bucket = self.buckets.get(name, None)
        if bucket:
            return bucket
        bucket = Couchbase.connect(
            host=self.conf.ip, port=self.conf.port, bucket=name,
            username=self.conf.user, password=self.conf.passwd)
        self.buckets[name] = bucket
        return bucket

    def create_ddoc(self, bucket, ddoc_name):
        """
        DESIGN = {
            '_id' : '_design/search_keywords',
            'language' : 'javascript',
            'views' : {
                'top_keywords' : {
                    'map' :
                    function(doc) {
                        if (typeof doc === 'number') {
                            emit(doc, null);
                        }
                    }
                }
            }
        }
        """
        view_names = self.ddocs[ddoc_name]
        doc_id = "_design/%s" % ddoc_name
        doc_views = {}
        for view_name in view_names:
            view_item = self.views[ddoc_name + '/' + view_name]
            item = {
                "map": view_item['map']
            }
            if view_item['reduce']:
                item['reduce'] = view_item['reduce']
            doc_views[view_name] = item
        ddoc = {'_id': doc_id, 'language': 'javascript', 'views': doc_views}
        bucket._design(ddoc_name, ddoc)

    def scan_ddoc(self, folder):
        self.ddocs = {}
        self.views = {}
        for cpath, folders, files in os.walk(folder):
            for ifile in files:
                if ifile.endswith('.js'):
                    ifile = os.path.join(cpath, ifile)
                    with open(ifile) as f:
                        view_content = f.read()
                    ifile = ifile.replace(folder, '')
                    # beer\sample-map.js
                    ifile = filter(None, ifile.replace('/', '\\').split('\\'))
                    logger.info(ifile)
                    ddoc_name = ifile[0]  # beer
                    view_name, func_name = ifile[-1].replace(
                        '.js', '').split('-')  # [sample,map]
                    key = ddoc_name + '/' + view_name
                    if key not in self.views:
                        view_item = {
                            u'ddoc_name': unicode(ddoc_name),
                            u'view_name': unicode(view_name),
                            u'map': None,
                            u'reduce': None
                        }
                    else:
                        view_item = self.views[key]
                    view_item[func_name] = unicode(view_content)
                    self.views[key] = view_item
                    self.ddocs.setdefault(view_item['ddoc_name'], [])
                    self.ddocs[view_item['ddoc_name']].append(view_name)

        for name in self.ddocs:
            logger.info(name)


@route('/admin/couchbase/ddoc', package=False)
class DdocController(Controller):
    require_auth = True

    def get(self):
        name = self.get_argument('name', None)
        cbs = self.application.plugins.couchbase
        cbs.scan_ddoc(os.path.join(self.application.root, 'ddoc'))
        if name is not None:
            model = getattr(self.models, name)
            ddoc_name = model.get_ddoc_name()
            cbs.create_ddoc(model.bucket, ddoc_name)
            self.write("create ddoc: %s " % ddoc_name)
        else:
            for model in self.models:
                ddoc_name = model.get_ddoc_name()
                cbs.create_ddoc(model.bucket, ddoc_name)
                self.write("create ddoc: %s " % ddoc_name)


class CouchbaseModel(EntityModel):
    bucket_name = None
    ddoc_name = None
    U0FFF = "\u0fff"
    U0000 = "\u0000"

    def __init__(self, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])

    def update(self, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])
        self.update_at = datetime.now()
        self.__class__.save(self)

    @classmethod
    def init(clz, application):
        EntityModel.init(application)
        if not hasattr(clz, '__res_name__'):
            return
        if clz.bucket_name is None:
            clz.bucket_name = application.settings.couchbase.bucket
        setattr(clz, 'bucket', application.cbb(clz.bucket_name))
        if application.settings.couchbase.init:
            ddoc_name = clz.get_ddoc_name()
            application.plugins.couchbase.create_ddoc(clz.bucket, ddoc_name)
            logger.info("create ddoc: %s " % clz.get_ddoc_name())

    @classmethod
    def pkid(clz, init=1):
        key = u'pk:o%s' % clz.__res_name__
        rv = clz.bucket.incr(key, initial=init)
        return rv.value

    @classmethod
    def get_ddoc_name(clz):
        if clz.ddoc_name:
            ddoc_name = clz.ddoc_name
        else:
            ddoc_name = getattr(clz, '__res_name__', '')
        return ddoc_name

    @classmethod
    def query(clz):
        return CouchQuery(clz, clz.bucket)
    
    @classmethod
    def _get(clz, key):
        try:
            rv = clz.bucket.get(key)
            return rv.value
        except Exception as ex:
            logger.error("%s, %s" % (key, ex))
            return None
    
    @classmethod
    def _set(clz, key, value, exp=0, flags=0, new=False):
        if new:
            clz.bucket.add(key, value, ttl=exp)
        else:
            clz.bucket.set(key, value, ttl=exp)
    
    @classmethod
    def _add(clz, key, value, exp=0, flags=0):
        clz.bucket.add(key, value, ttl=exp)

    @classmethod
    def _incr(clz, key, init=1):
        rv = clz.bucket.incr(key, initial=init)
        return rv.value

    @classmethod
    def find(clz, id, time=86400, update=False):
        key = u'%s:%s' % (clz.__res_name__, id.__str__())
        rv = clz._get(key)
        if clz.app.debug:
            logger.debug('find-byid = ' + key + " >> " + str(rv))
        if rv is not None:
            if isinstance(rv, dict):
                return clz(**rv)
            else:
                return clz.from_json(rv)
        return None

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
    def find_view(clz, *args, **kwargs):
        """
        {u'reason': u'invalid UTF-8 JSON: {{error,{1,"lexical error: invalid char in json text."}},"admin"}', u'error': u'bad_request'}
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
            ddoc = args[1]
        else:
            ddoc = clz.get_ddoc_name()
        rvt = clz.bucket._view(ddoc, args[0], kwargs)
        if clz.app.debug:
            logger.debug(rvt.value)
        if 'error' in rvt.value:
            raise Exception('find view error, ddoc=%s, view=%s, params=%s, reason:%s' % (
                ddoc, args[0], str(kwargs), rvt.value['reason']))
        return rvt.value

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
        limit = kwargs.get('limit', 20)
        page = kwargs.get('page', 1)
        idfmap = kwargs.pop('fmap', long)
        results = clz.find_view(*args, **kwargs)
        total = results['total_rows']
        itemids = []
        for item in results['rows']:
            itemids.append(item['id'].split(':')[-1])
        if clz.app.debug:
            logger.debug(str(total) + ','.join(itemids))
        rs = RowSet(itemids, clz, total=total,
                    limit=limit, start=page, fmap=idfmap)
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
        {"rows":[
            {"key":["a7b5351eb5ca297d0c6ec7a9b5020ef3","5"],"value":1},
            {"key":["a7b5351eb5ca297d0c6ec7a9b5020ef3","14"],"value":3}
            ]
        }
        link:
        http://www.couchbase.com/docs/couchbase-manual-2.0/couchbase-views-writing-querying-selection.html
        """
        kwargs['group'] = True
        kwargs['reduce'] = True
        rvt = clz.find_view(*args, **kwargs)
        return rvt['rows']

    @classmethod
    def get_stat(clz, *args, **kwargs):
        kwargs['limit'] = 1
        rs = clz.find_stats(*args, **kwargs)
        if len(rs) > 0:
            return rs[0].value
        return None

    @classmethod
    def create(clz, *args, **kwargs):
        """
        args[0] = clz(),
        kwargs = {'time':86000 or None}
        """
        o = args[0]
        if not hasattr(o, 'id') or not o.id:
            o.id = clz.pkid()
        o.doc_type = clz.__res_name__
        if not kwargs:
            kwargs = {}
        exp = kwargs.get('time', 0)
        flags = kwargs.get('flags', 0)
        key = '%s:%s' % (clz.__res_name__, o.id)
        clz._set(key, o, exp=exp, flags=flags, new=True)
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
        clz._set(key, o, exp=exp, flags=flags)
        return True

    @classmethod
    def remove(clz, *args, **kwargs):
        """
        args[0] = id
        """
        flag = kwargs.get('erase', False)
        if flag:
            key = u'%s:%s' % (clz.__res_name__, args[0])
            clz.bucket.delete(key)
        else:
            cmt = clz.find(args[0])
            cmt.if_deleted = 1
            cmt.delete_at = datetime.now()
            clz.save(cmt)


def date_to_array(date):
    """
    return [year, month, day, hour, minute, second]
    """
    if date is None:
        return
    return [date.year, date.month, date.day, date.hour, date.minute, date.second]


class CouchQuery(object):
    U0FFF = "\u0fff"
    U0000 = "\u0000"

    def __init__(self, model, bucket):
        self.bucket = bucket
        self.model = model
        self.q = {'limit': 20, 'page': 1}

    def limit(self, count):
        self.q['limit'] = count
        return self

    def page(self, index):
        index = int(index)
        count = self.q.get('limit', 0) * (index - 1)
        self.q['skip'] = count
        return self

    def key(self, value):
        if not value.startswith('"'):
            value = '"' + value + '"'
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

    def view(self):
        pass
