#!/usr/bin/env python
# -*- coding: utf-8 -*-

# -*- coding: utf-8 -*-
import roo.log
logger = roo.log.logger(__name__)

import os
from datetime import datetime

import couchbase
from roo.lib import jsonfy
couchbase.set_json_converters(jsonfy.dumps, jsonfy.loads)

import tornado.web
"""
import tornado.platform.twisted
tornado.platform.twisted.install()
from twisted.internet import reactor
"""
from twisted.python.failure import Failure
from twisted.internet.defer import inlineCallbacks, Deferred
# for 1.2.0
from couchbase import experimental
experimental.enable()
from txcouchbase.connection import Connection as TxCouchbase


from roo.plugin import BasePlugin, plugin
from roo.model import EntityModel
from roo.collections import RowSet
from roo.controller import Controller
from roo.router import route
from roo import pools


class BucketPool(pools.Pool):

    def make_instance(self):
        #return Couchbase.connect(**self.args)
        return TxCouchbase(**self.args)


def parse_failure(error):
    #print error.__class__
    #print error
    err = error.value
    if hasattr(err, 'objextra'):
        err = err.objextra
    #print 'error:', err
    if hasattr(err, 'value'):
        return err.value
    else:
        return err


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

    def create_ddoc(self, bucket, ddoc_name, call_back):
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
            d = c.design_create(ddoc_name, ddoc, use_devmode=False, syncwait=5)
            d.addCallback(call_back)
            d.addErrback(call_back)
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

    @tornado.web.asynchronous
    def post(self):
        name = self.get_argument('name', None)
        cbs = self.application.plugins.couchbase
        cbs.scan_ddoc(os.path.join(self.application.root, 'ddoc'))
        if name is not None:
            model = getattr(self.models, name)
            ddoc_name = model.get_ddoc_name()
            cbs.create_ddoc(model.bucket, ddoc_name, self.on_create_ddoc)
        else:
            for model in self.models:
                ddoc_name = model.get_ddoc_name()
                cbs.create_ddoc(model.bucket, ddoc_name, self.on_create_ddoc)
    
    def on_create_ddoc(self, result):
        if isinstance(result, Failure):
            err = parse_failure(result)
            self.write_verror(msg="create ddoc error. %s" % err)
        else:
            self.write_ok(msg="create ddoc done.")
        self.finish()


@route('/admin/couchbase/views', package=False)
class DdocViewsController(Controller):
    require_auth = True

    def get(self):
        rs = self.application.plugins.couchbase.ddocs
        self.xrender(rs=rs)


@route('/admin/couchbase/json/(?P<id>[a-zA-Z0-9:_]+)', package=False)
class DdocJSonController(Controller):
    require_auth = True
    
    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        cbs = self.application.plugins.couchbase
        id = kwargs.get('id')
        if len(id) == 0:
            self.write("")
        model = id.split(':')[0]
        cb = cbs.get_bucket(model)
        with cb.reserve() as c:
            d = c.get(id)
            d.addCallback(self.on_get)
            d.addErrback(self.on_async_error)
    
    def on_get(self, ret):
        self.write(jsonfy.dumps(ret.value))
        self.finish()
    
    def on_async_error(self, error):
        logger.error("%s" % error)
        err = error.value
        if hasattr(err, 'objextra'):
            err = err.objextra
        print 'error:', err
        if hasattr(err, 'value'):
            self.write_verror(errors=[err.value])
        else:
            self.write_verror(msg=str(err))
        self.finish()


@route('/admin/couchbase/touch/(?P<id>[a-zA-Z0-9:_]+)', package=False)
class DdocTouchController(Controller):
    require_auth = True
    
    @tornado.web.asynchronous
    def get(self, *args, **kwargs):
        cbs = self.application.plugins.couchbase
        id = kwargs.get('id')
        if len(id) == 0:
            self.write("")
        model = id.split(':')[0]
        cb = cbs.get_bucket(model)
        with cb.reserve() as c:
            d = c.touch(id, ttl=10)
            d.addCallback(self.on_touch)
            d.addErrback(self.on_async_error)
    
    def on_touch(self, ret):
        self.write("Done")
        self.finish()
    
    def on_async_error(self, error):
        logger.error("%s" % error)
        err = error.value
        if hasattr(err, 'objextra'):
            err = err.objextra
        print 'error:', err
        if hasattr(err, 'value'):
            self.write_verror(errors=[err.value])
        else:
            self.write_verror(msg=str(err))
        self.finish()


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
    def pkid(clz, call_back, init=1):
        def _async_callback(rv):
            call_back(rv.value)

        def _async_errback(rv):
            logger.exception(rv)
            call_back(None)
        key = u'pk:o%s' % clz.__res_name__
        with clz.bucket.reserve() as c:
            d = c.incr(key, initial=init)
            d.addCallback(_async_callback)
            d.addErrback(_async_errback)
            #return rv.value

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
    def _get(clz, key, call_back, quiet=True):
        def _async_callback(rv):
            if clz.app.debug:
                logger.debug("_get(%s):%s", key, rv)
            if rv.success:
                call_back(rv.value)
            else:
                call_back(None)
            
        def _async_errback(rv):
            logger.exception(rv)
            call_back(None)

        with clz.bucket.reserve() as c:
            d = c.get(key, quiet=quiet)
            d.addCallback(_async_callback)
            d.addErrback(_async_errback)

    @classmethod
    def _getm(clz, keys, call_back, quiet=True, clazz=None):
        if keys is None or len(keys) == 0:
            call_back({})
            return

        def _async_callback(rv):
            if clz.app.debug:
                logger.debug("_get(%s):%s", keys, rv)
            ret = {}
            for key in keys:
                item = rv.get(key, None)
                ret[key] = None
                if item:
                    if clazz:
                        ret[key] = to_clazz(clazz, item.value)
                    else:
                        ret[key] = item.value
            call_back(ret)
            
        def _async_errback(rv):
            logger.exception(rv)
            call_back(None)

        with clz.bucket.reserve() as c:
            d = c.get_multi(keys, quiet=quiet)
            d.addCallback(_async_callback)
            d.addErrback(_async_errback)

    @classmethod
    def _set(clz, key, value, callback=None, exp=0, flags=0, new=False, format=couchbase.FMT_JSON):
        def _async_callback(rv):
            if callback:
                callback(value)

        def _async_errback(rv):
            logger.error('_set key=' + key)
            logger.exception(rv)
            if callback:
                callback(None)

        with clz.bucket.reserve() as c:
            if new:
                d = c.add(key, value, ttl=exp, format=format)
            else:
                d = c.set(key, value, ttl=exp, format=format)
            d.addCallback(_async_callback)
            d.addErrback(_async_errback)

    @classmethod
    def _add(clz, key, value, exp=0, flags=0, format=couchbase.FMT_JSON):
        def _async_callback(rv):
            pass

        def _async_errback(rv):
            logger.error('_add key=' + key)
            logger.exception(rv)

        with clz.bucket.reserve() as c:
            d = c.add(key, value, ttl=exp, format=couchbase.FMT_JSON)
            d.addCallback(_async_callback)
            d.addErrback(_async_errback)

    @classmethod
    def _incr(clz, key, callback, init=1):
        def _async_callback(rv):
            callback(rv.value)

        def _async_errback(rv):
            logger.error('_incr key=' + key)
            logger.exception(rv)

        with clz.bucket.reserve() as c:
            d = c.incr(key, initial=init)
            d.addCallback(_async_callback)
            d.addErrback(_async_errback)
    
    @property
    def cbkey(self):
        key = u'%s:%s' % (self.__class__.__res_name__, self.id.__str__())
        return key

    @classmethod
    def find(clz, id, callback, time=86400, update=False):
        def _async_callback(rv):
            if rv is not None:
                if isinstance(rv, dict):
                    callback(clz(**rv))
                else:
                    callback(clz.from_json(rv))
            else:
                callback(None)

        key = u'%s:%s' % (clz.__res_name__, id.__str__())
        clz._get(key, _async_callback)
    
    @classmethod
    def find_multi(clz, itemids, callback):
        keys = [u'%s:%s' % (clz.__res_name__, kid.__str__()) for kid in itemids]

        def _async_callback(ms):
            if ms:
                items = [ms.get(iid, None) for iid in keys]
                total = len(itemids)
            else:
                logger.error('ms is None or empty')
                items = []
                total = 0
            rs = RowSet(items, None, total=total, limit=-1, start=1, fmap=None)
            callback(rs)
        
        clz._getm(keys, _async_callback, clazz=clz)

    @classmethod
    def find_one(clz, *args, **kwargs):
        """
        args: (view_name, ddoc_name)
        kwargs : {'key1':'value1','key2':'value2'}
        """
        callback = kwargs.pop('callback', None)

        def _async_callback(rv):
            if rv:
                callback(rv.pop())
            else:
                callback(None)

        kwargs['limit'] = 1
        kwargs['callback'] = _async_callback
        clz.find_list(*args, **kwargs)

    @classmethod
    def find_view(clz, *args, **kwargs):
        """
        queryAll return Result BatchedView<Design=beer, View=brewery_b
eers, Query=Query:'limit=20', Rows Fetched=20>

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
        view_name = args[0]
        if len(args) >= 2:
            ddoc = args[1]
        else:
            ddoc = clz.get_ddoc_name()
        if clz.app.debug:
            logger.debug('ddoc: ' + ddoc)
            logger.debug(kwargs)
        
        callback = kwargs.get('callback', None)
        if callback is None:
            raise Exception(" callback argument is None. ")

        def _async_callback(rv):
            if clz.app.debug:
                logger.debug(rv)
            callback(rv)

        def _async_errback(rv):
            logger.error('find view error, ddoc=%s, view=%s, params=%s' % (ddoc, view_name, str(kwargs)))
            logger.exception(rv)
            callback(None)

        with clz.bucket.reserve() as c:
            d = c.queryAll(ddoc, view_name, **kwargs)
            d.addCallback(_async_callback)
            d.addErrback(_async_errback)
    
    @classmethod
    def count_list(clz, *args, **kwargs):
        kwargs.pop('lazy', False)
        kwargs['limit'] = 0
        kwargs['page'] = 1
        kwargs['reduce'] = True
        kwargs['group'] = True
        kwargs.pop('fmap', long)

        callback = kwargs.pop('callback', None)

        def _async_callback(rv):
            rs = [item.value for item in rv]
            if len(rs) > 0:
                callback(rs[0])
            else:
                callback(0)

        kwargs['callback'] = _async_callback
        clz.find_view(*args, **kwargs)
        
    @classmethod
    def find_list(clz, *args, **kwargs):
        """
        args: (view_name, ddoc_name)
        kwargs : {
            'limit':'20','page':'value2', 'descending': true|false,
            "startkey": ABC, "endkey": DEF,
            "key": ABC, "keys": []
            }
        """
        #lazy = kwargs.pop('lazy', True)
        limit = kwargs.get('limit', 20)
        page = kwargs.get('page', 1)
        #idfmap = kwargs.pop('fmap', long)
        callback = kwargs.pop('callback', None)

        def _async_callback(rows):
            """
            BatchedView
            ViewRow(key=[u'357'], value=None, docid=u'357', doc=None)
            """
            if rows:
                itemids = [item.docid for item in rows]
                #indexed_rows = rows.indexed_rows

                def _async_callback2(ms):
                    items = [ms.get(iid, None) for iid in itemids]
                    rs = RowSet(items, None, total=len(itemids), limit=limit, start=page, fmap=None)
                    callback(rs)

                clz._getm(itemids, _async_callback2, clazz=clz)

            else:
                callback(None)

        kwargs['callback'] = _async_callback
        clz.find_view(*args, **kwargs)

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
        """
        kwargs['group'] = True
        kwargs['reduce'] = True
        clz.find_view(*args, **kwargs)

    @classmethod
    def get_stat(clz, *args, **kwargs):
        callback = kwargs.pop('callback', None)

        def _async_callback(rv):
            if clz.app.debug:
                logger.debug('get_stat: ' + rv)
                logger.debug(kwargs)
            rs = [item.value for item in rv]
            if len(rs) > 0:
                callback(rs[0])
            else:
                callback({})

        kwargs['limit'] = 1
        kwargs['callback'] = _async_callback
        clz.find_stats(*args, **kwargs)

    @classmethod
    def create(clz, *args, **kwargs):
        """
        args[0] = clz(),
        kwargs = {'time':86000 or None}
        """
        o = args[0]
        o.doc_type = clz.__res_name__
        if not kwargs:
            kwargs = {}
        exp = kwargs.get('time', 0)
        flags = kwargs.get('flags', 0)
        callback = kwargs.pop('callback', None)

        def _async_callback(oid):
            o.id = oid
            clz._set(o.cbkey, o, callback=callback, exp=exp, flags=flags, new=True)

        if not hasattr(o, 'id') or not o.id:
            clz.pkid(_async_callback)
        else:
            clz._set(o.cbkey, o, callback=callback, exp=exp, flags=flags, new=True)

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
        callback = kwargs.get('callback', None)
        clz._set(o.cbkey, o, callback=callback, exp=exp, flags=flags)

    @classmethod
    def remove(clz, *args, **kwargs):
        """
        args[0] = id
        """
        def _async_callback(cmt):
            cmt.if_deleted = 1
            cmt.delete_at = datetime.now()
            clz.save(cmt)
            
            flag = kwargs.get('erase', False)
            if flag:
                cmt.ttl(30)

        clz.find(args[0], _async_callback)
    
    def ttl(self, ttl=3600 * 24):
        """
        ttl in a week
        """
        def _async_callback(rv):
            pass

        def _async_errback(rv):
            logger.exception(rv)

        clz = self.__class__
        with clz.bucket.reserve() as c:
            d = c.touch(self.cbkey, ttl=ttl)
            d.addCallback(_async_callback)
            d.addErrback(_async_errback)


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

    def descending(self):
        self.q['descending'] = True
        return self

    def view(self):
        pass
    
    def stale(self, value='update_after'):
        self.q['stale'] = value
        return self
        
    def lazy(self, value):
        self.q['lazy'] = value
        return self
