# -*- coding: utf-8 -*-
import roo.log
logger = roo.log.logger(__name__)

import os
from datetime import datetime
import couchbase
from couchbase import Couchbase
from roo.lib import jsonfy
couchbase.set_json_converters(jsonfy.dumps, jsonfy.loads)

from roo.plugin import BasePlugin, plugin
from roo.model import EntityModel
from roo.collections import RowSet
from roo.controller import Controller
from roo.router import route
from roo import pools


class BucketPool(pools.Pool):

    def make_instance(self):
        return Couchbase.connect(**self.args)


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
        args = {}
        args['host'] = self.conf.ip
        args['port'] = self.conf.port
        #args['lockmode'] = LOCKMODE_WAIT
        args['bucket'] = name
        if self.conf.user:
            args['username'] = self.conf.user
        if self.conf.passwd:
            args['password'] = self.conf.passwd
        slots = self.conf.get('slots', 5)
        bucket = BucketPool(name, args, n_slots=slots)
        #logger.info('default_format:%s', bucket.default_format)
        self.buckets[name] = bucket
        return bucket
    
    def map_bucket(self, name, bucket):
        self.buckets[name] = bucket

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
        if ddoc_name not in self.ddocs:
            logger.info("create ddoc: %s None. " % ddoc_name)
            return
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
        with bucket.reserve() as c:
            ret = c.design_create(ddoc_name, ddoc, use_devmode=False, syncwait=5)
            logger.info("create ddoc: %s, %s, %s" % (
                ddoc_name, ', '.join(doc_views.keys()), ret))

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

    def post(self):
        name = self.get_argument('name', None)
        cbs = self.application.plugins.couchbase
        cbs.scan_ddoc(os.path.join(self.application.root, 'ddoc'))
        if name is not None:
            model = getattr(self.models, name)
            ddoc_name = model.get_ddoc_name()
            cbs.create_ddoc(model.bucket, ddoc_name)
            self.write_ok(msg="create ddoc: %s " % ddoc_name)
        else:
            for model in self.models:
                ddoc_name = model.get_ddoc_name()
                cbs.create_ddoc(model.bucket, ddoc_name)
                self.write_ok(msg="create ddoc: %s " % ddoc_name)


@route('/admin/couchbase/views', package=False)
class DdocViewsController(Controller):
    require_auth = True

    def get(self):
        rs = self.application.plugins.couchbase.ddocs
        self.xrender(rs=rs)


@route('/admin/couchbase/json/(?P<id>[a-zA-Z0-9:_]+)', package=False)
class DdocJSonController(Controller):
    require_auth = True

    def get(self, *args, **kwargs):
        cbs = self.application.plugins.couchbase
        id = kwargs.get('id')
        if len(id) == 0:
            self.write("")
        model = id.split(':')[0]
        cb = cbs.get_bucket(model)
        with cb.reserve() as c:
            try:
                rv = c.get(id)
                self.write(jsonfy.dumps(rv.value))
            except Exception as ex:
                logger.error("%s, %s" % (id, ex))
                self.write("Error")


@route('/admin/couchbase/touch/(?P<id>[a-zA-Z0-9:_]+)', package=False)
class DdocTouchController(Controller):
    require_auth = True

    def get(self, *args, **kwargs):
        cbs = self.application.plugins.couchbase
        id = kwargs.get('id')
        if len(id) == 0:
            self.write("")
        model = id.split(':')[0]
        cb = cbs.get_bucket(model)
        with cb.reserve() as c:
            try:
                c.touch(id, ttl=10)
                self.write("Done")
            except Exception as ex:
                logger.error("%s, %s" % (id, ex))
                self.write("Error")


def to_clazz(clazz, value):
    if isinstance(value, dict):
        return clazz(**value)
    else:
        return clazz.from_json(value)


class CouchbaseModel(EntityModel):
    bucket_name = None
    ddoc_name = None
    KEY_STR_END = '"\u0fff"'
    KEY_STR_BEGIN = '"\u0000"'

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
        bmap = application.settings.couchbase.bucket
        _default = bmap.get('default')
        _bucket_name = bmap.get(clz.__res_name__, _default)
        if clz.bucket_name is None:
            clz.bucket_name = _bucket_name
        setattr(clz, 'bucket', application.cbb(clz.bucket_name))
        application.plugins.couchbase.map_bucket(clz.__res_name__, clz.bucket)
        if application.settings.couchbase.init:
            ddoc_name = clz.get_ddoc_name()
            application.plugins.couchbase.create_ddoc(clz.bucket, ddoc_name)
            logger.info("create ddoc: %s " % clz.get_ddoc_name())

    @classmethod
    def pkid(clz, init=1):
        key = u'pk:o%s' % clz.__res_name__
        with clz.bucket.reserve() as c:
            rv = c.incr(key, initial=init)
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
    def _get(clz, key, quiet=True):
        with clz.bucket.reserve() as c:
            try:
                rv = c.get(key, quiet=quiet)
                if clz.app.debug:
                    logger.debug("_get(%s):%s", key, rv)
                if rv.success:
                    return rv.value
                return None
            except Exception as ex:
                logger.error("_get %s" % key)
                logger.exception(ex)
                return None

    @classmethod
    def _getm(clz, keys, quiet=True, clazz=None):
        if keys is None or len(keys) == 0:
            return {}
        with clz.bucket.reserve() as c:
            try:
                rv = c.get_multi(keys, quiet=quiet)
                if clz.app.debug:
                    logger.debug("_get(%s):%s", keys, rv)
                if rv.all_ok:
                    ret = {}
                    for key in keys:
                        item = rv.get(key, None)
                        ret[key] = None
                        if item:
                            if clazz:
                                ret[key] = to_clazz(clazz, item.value)
                            else:
                                ret[key] = item.value
                    return ret
                return None
            except Exception as ex:
                logger.error("%s, %s" % (keys, ex))
                logger.exception(ex)
                return None

    @classmethod
    def _set(clz, key, value, exp=0, flags=0, new=False, format=couchbase.FMT_JSON):
        with clz.bucket.reserve() as c:
            if new:
                c.add(key, value, ttl=exp, format=format)
            else:
                c.set(key, value, ttl=exp, format=format)

    @classmethod
    def _add(clz, key, value, exp=0, flags=0, format=couchbase.FMT_JSON):
        with clz.bucket.reserve() as c:
            c.add(key, value, ttl=exp, format=couchbase.FMT_JSON)

    @classmethod
    def _incr(clz, key, init=1):
        with clz.bucket.reserve() as c:
            rv = c.incr(key, initial=init)
            return rv.value
    
    @property
    def cbkey(self):
        key = u'%s:%s' % (self.__class__.__res_name__, self.id.__str__())
        return key

    @classmethod
    def find(clz, id, time=86400, update=False):
        key = u'%s:%s' % (clz.__res_name__, id.__str__())
        rv = clz._get(key)
        #if clz.app.debug:
        #    logger.debug('find-byid = ' + key + " >> " + str(rv))
        if rv is not None:
            if isinstance(rv, dict):
                return clz(**rv)
            else:
                return clz.from_json(rv)
        return None
    
    @classmethod
    def find_multi(clz, itemids):
        if itemids:
            keys = [u'%s:%s' % (clz.__res_name__, kid.__str__()) for kid in itemids]
            ms = clz._getm(keys, clazz=clz)
            if ms:
                items = [ms.get(iid, None) for iid in keys]
                total = len(itemids)
            else:
                logger.error('ms is None or empty')
                items = []
                total = 0
        else:
            total = 0
            items = []
        rs = RowSet(items, None, total=total,
                    limit=-1, start=1, fmap=None)
        return rs

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
        if clz.app.debug:
            logger.debug('ddoc: ' + ddoc)
            logger.debug(kwargs)
        with clz.bucket.reserve() as c:
            rvt = c._view(ddoc, args[0], params=kwargs)
            if clz.app.debug:
                logger.debug(rvt.value)
            if 'error' in rvt.value:
                raise Exception('find view error, ddoc=%s, view=%s, params=%s, reason:%s' % (
                    ddoc, args[0], str(kwargs), rvt.value['reason']))
            return rvt.value
    
    @classmethod
    def count_list(clz, *args, **kwargs):
        kwargs.pop('lazy', False)
        kwargs['limit'] = 0
        kwargs['page'] = 1
        kwargs['reduce'] = True
        kwargs['group'] = True
        kwargs.pop('fmap', long)
        rvt = clz.find_view(*args, **kwargs)
        nums = rvt['rows']
        if nums:
            return nums[0].get('value', 0)
        return 0
        
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
        lazy = kwargs.pop('lazy', True)
        limit = kwargs.get('limit', 20)
        page = kwargs.get('page', 1)
        idfmap = kwargs.pop('fmap', long)
        results = clz.find_view(*args, **kwargs)
        total = results.get('total_rows', 0)
        if not lazy:
            itemids = [item['id'] for item in results['rows']]
            ms = clz._getm(itemids, clazz=clz)
            items = [ms.get(iid, None) for iid in itemids]
            rs = RowSet(items, None, total=total,
                        limit=limit, start=page, fmap=None)
            return rs
        else:
            itemids = []
            for item in results['rows']:
                itemids.append(item['id'].split(':')[-1])
            if clz.app.debug:
                logger.debug(str(total) + ' / ' + ','.join(itemids))
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
            return rs[0]['value']
        return {}

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
        clz._set(o.cbkey, o, exp=exp, flags=flags, new=True)
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
        o.doc_type = clz.__res_name__
        exp = kwargs.get('time', 0)
        flags = kwargs.get('flags', 0)
        clz._set(o.cbkey, o, exp=exp, flags=flags)
        return True

    @classmethod
    def remove(clz, *args, **kwargs):
        """
        args[0] = id
        """
        cmt = clz.find(args[0])
        cmt.if_deleted = 1
        cmt.delete_at = datetime.now()
        clz.save(cmt)

        flag = kwargs.get('erase', False)
        if flag:
            with clz.bucket.reserve() as c:
                c.touch(cmt.cbkey, ttl=30)
    
    def ttl(self, ttl=3600 * 24):
        """
        ttl in a week
        """
        clz = self.__class__
        with clz.bucket.reserve() as c:
            c.touch(self.cbkey, ttl=ttl)


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
        self.q['page'] = index
        return self

    def key(self, value):
        if value is None:
            return self
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
        self.q['reduce'] = value
        return self

    def sort(self, descending=True):
        self.q['descending'] = descending
        return self

    def view(self):
        pass
    
    def stale(self, value='update_after'):
        self.q['stale'] = value
        return self
        
    def lazy(self, value):
        self.q['lazy'] = value
        return self
