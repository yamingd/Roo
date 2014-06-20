# -*- coding: utf-8 -*-
import roo.log
logger = roo.log.logger(__name__)

import re
from datetime import datetime
import urllib
import mimetypes

import tornado.web
import tornado.ioloop

from concurrent.futures import ThreadPoolExecutor
from functools import partial, wraps

from roo import threadlocal
from roo.lib import jsonfy
from roo.router import route
from roo.collections import RowSet
from roo.config import settings

cthread = hasattr(settings, 'thread')
if cthread:
    EXECUTOR = ThreadPoolExecutor(max_workers=settings.thread.get('workers', 10))
else:
    EXECUTOR = None
    logger.info(u"unblock is disabled, please config settings.thread to enable it.")


def unblock(f):

    @tornado.web.asynchronous
    @wraps(f)
    def wrapper(*args, **kwargs):
        self = args[0]

        def _call_func(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except Exception, e:
                logger.exception(e)
                raise e
        
        def callback(future):
            """
            future.result() => {'data':[], 'msg':u'abc'}
            """
            ret = future.result()
            if ret is None:
                self.finish()
            elif isinstance(ret, dict):
                if 'errors' in ret:
                    self.write_verror(**ret)
                else:
                    self.write_ok(**ret)
                self.finish()
            else:
                self.write(ret)
                self.finish()
                
        EXECUTOR.submit(
            partial(_call_func, *args, **kwargs)
        ).add_done_callback(
            lambda future: tornado.ioloop.IOLoop.instance().add_callback(
                partial(callback, future)))

    return wrapper


class Controller(tornado.web.RequestHandler):
    require_auth_action = []
    require_auth = False

    def __init__(self, application, request, transforms=None):
        self.render_context = {}
        self.action_method = None
        self.current_date = datetime.now()
        tornado.web.RequestHandler.__init__(self, application, request)

    def get_req_format(self):
        accept = self.request.headers.get("accept", None)
        if not accept or len(accept) == 0:
            return "html"
        if accept.find("application/xhtml") >= 0 or accept.find("text/html") >= 0:
            return "html"
        if accept.find("application/xml") >= 0 or accept.find("text/xml") >= 0:
            return "xml"
        if accept.find("text/plain") >= 0:
            return "txt"
        if accept.find("application/json") >= 0 or accept.find("text/javascript") >= 0:
            return "json"
        return "html"

    def get_template_file(self):
        """
        located by controller, method and output format.
        """
        folder, _ = route._url_segs(self.__class__)
        logger.debug('template file: %s' % folder)
        if folder.startswith('/'):
            folder = folder[1:]
        if folder.startswith('/'):
            folder = folder[1:]
        tmpl_name = self.action_method
        if tmpl_name:
            tmpl_name = '/' + tmpl_name.lower()
        else:
            tmpl_name = ''
        format = self.get_req_format()
        return "%s%s.%s" % (folder, tmpl_name, format)

    @property
    def is_xhr(self):
        h = self.request.headers.get('X-Requested-With', None)
        return h and h == 'XMLHttpRequest'

    @property
    def models(self):
        return self.application.models

    @property
    def config(self):
        return self.application.settings

    def add_render_args(self, **kwargs):
        """
        add render data args
        """
        self.render_context.update(kwargs)

    def get_template_namespace(self):
        """
        override to prepare template rending datas.
        """
        kwargs = tornado.web.RequestHandler.get_template_namespace(self)
        kwargs.update(self.render_context)
        kwargs['conf'] = self.application.settings
        kwargs['nl2pbr'] = self.nl2pbr
        return kwargs

    def xrender(self, **kwargs):
        """
        kwargs is view data
        output html / json / xml content.
        use this instead of either render or render_string
        """
        template_name = self.get_template_file()
        logger.debug('xrender: %s' % template_name)
        return tornado.web.RequestHandler.render(self, template_name, **kwargs)

    def prepare(self):
        """
        Called at the beginning of a request before `get`/`post`/etc.
        Override this method to perform common initialization regardless
        of the request method.
        """
        for plugin in self.enabled_plugins:
            plugin.on_before(self)

    @property
    def enabled_plugins(self):
        return self.application.enabled_plugins

    def on_finish(self):
        """Called after the end of a request.

        Override this method to perform cleanup, logging, etc.
        This method is a counterpart to `prepare`.  ``on_finish`` may
        not produce any output, as it is called after the response
        has been sent to the client.
        """
        for plugin in self.enabled_plugins:
            plugin.on_finished(self)

    def _handle_request_exception(self, e):
        """
        override to handle Exception
        """
        for plugin in self.enabled_plugins:
            plugin.on_exception(self, e)
        tornado.web.RequestHandler._handle_request_exception(self, e)

    def get_current_user(self):
        """
        Override to determine the current user from, e.g., a cookie.
        use SessionPlugin to init current user from cookie and then save to threadlocal
        """
        if not hasattr(self, '_current_user'):
            self._current_user = threadlocal.get_user()
        return self._current_user

    def remember_user(self, user, remember_me):
        """
        set current user and store user-id to cookie
        """
        threadlocal.set_user(user)
        session_config = self.config.session
        cookie_id = str(session_config.auth_cookie)
        if user:
            self._current_user = user
            if remember_me:
                self.set_secure_cookie(cookie_id, str(
                    user.id), expires_days=session_config.expires_days)
            else:
                self.set_secure_cookie(cookie_id, str(
                    user.id), expires_days=None)
        else:
            self.set_secure_cookie(cookie_id, str(-1), expires_days=None)

    def clear_user(self):
        """
        called on user sign out.
        """
        session_config = self.config.session
        cookie_id = str(session_config.auth_cookie)
        self.clear_cookie(cookie_id)

    def abort(self, code='501', msg='abort'):
        """
        abort this request as security reason or validation.
        """
        raise tornado.web.HTTPError(code, msg)

    def _execute(self, transforms, *args, **kwargs):
        """Executes this request with the given output transforms."""
        self._transforms = transforms
        try:
            if self.request.method not in self.SUPPORTED_METHODS:
                raise tornado.web.HTTPError(405)
            self.path_args = [self.decode_argument(arg) for arg in args]
            self.path_kwargs = dict((k, self.decode_argument(v, name=k))
                                    for (k, v) in kwargs.items())
            # If XSRF cookies are turned on, reject form submissions without
            # the proper cookie
            if self.request.method not in ("GET", "HEAD", "OPTIONS") and \
                    self.application.settings.get("xsrf_cookies"):
                self.check_xsrf_cookie()
            self.prepare()
            if not self._finished:
                self._execute_method(*self.path_args, **self.path_kwargs)
                if self._auto_finish and not self._finished:
                    self.finish()
        except Exception as e:
            self._handle_request_exception(e)

    def _execute_method(self, *args, **kwargs):
        """

        Routes as follows:
        A: /person/([a-zA-Z]+)/([0-9]+)
        B: /person/(?P<action>[a-zA-Z]+)/(?P<id>[0-9]+)

        Request as follows:
        /hello/123

        for A: args = ("hello","123"), kwargs = {}
        for B: args = (), kwargs = {"action":"hello", "id": "123"}

        """
        method_name = self.request.method.lower()
        if 'action' in kwargs:
            method_name = kwargs['action']
            del kwargs['action']
            self.action_method = method_name
        func = getattr(self, method_name)
        func(*args, **kwargs)

    def _wrap_data(self, data=[], status=200, msg='', total=0, fmap=None):
        m = {}
        m['status'] = status
        m['msg'] = msg
        m['total'] = total
        if isinstance(data, dict):
            m['data'] = [data]
        elif isinstance(data, list):
            m['data'] = data
        elif isinstance(data, RowSet):
            m['data'] = data.as_map(fmap=fmap)
        else:
            m['data'] = []
        return m

    def write_ok(self, msg='OK', data=[], total=0):
        self.set_status(200)
        m = self._wrap_data(
            status=200, msg=msg, data=data, total=total)
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.write(jsonfy.dumps(m))

    def write_verror(self, msg='error', errors=[], status=601):
        self.set_status(500)
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.write(self._wrap_data(status=status, msg=msg, data=errors))

    def nl2pbr(self, s):
        """
        {{ nl2pbr }}

        Convert newlines into <p> and <br />s.
        """
        if not isinstance(s, basestring):
            s = str(s)
        s = re.sub(r'\r\n|\r|\n', '\n', s)
        paragraphs = re.split('\n{2,}', s)
        if len(paragraphs) <= 1:
            return s
        paragraphs = ['<p>%s</p>' % p.strip().replace(
            '\n', '<br />') for p in paragraphs]
        return ''.join(paragraphs)

    def get_path_intvalue(self, key, default=None):
        val = self.path_kwargs.get(key, default)
        if not isinstance(val, int) and len(val) == 0:
            return default
        return int(val)

    def get_path_value(self, key, default=None):
        val = self.path_kwargs.get(key, default)
        return val

    def get_long_arg(self, key, default=None):
        val = self.get_argument(key, None)
        if val is None or len(val) == 0:
            return default
        return long(val)

    def send_file(self, file_path):
        url = urllib.pathname2url(file_path)
        if self.application.debug:
            logger.debug(url)
        mtype, mcoding = mimetypes.guess_type(url)
        if mtype is None:
            mtype = "application/octet-stream"
        self.add_header("Content-Type", mtype)
        self.add_header("X-Accel-Redirect", url)


class UrlDebug(Controller):

    def get(self):
        for item in self.application.handlers[0][1]:
            self.write(str(item))
            self.write('<br />')
