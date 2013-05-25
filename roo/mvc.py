# -*- coding: utf-8 -*-
import re
import os
from datetime import datetime
import signal
import time
# tornado
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
from tornado import escape

try:
    import pycurl
    from tornado import httpclient
    httpclient.AsyncHTTPClient.configure(
        "tornado.curl_httpclient.CurlAsyncHTTPClient")
except:
    pass

# Roo
import roo.log
logger = roo.log.logger(__name__)

from roo import enum
from roo.router import route
from roo.plugin import manager as pm
from roo.model import EntityModel
from roo.lib.dictfy import ODict
from roo.controller import Controller, UrlDebug

SHUTDOWN_WAIT = 3


class RooApplication(tornado.web.Application):

    """
    Base Application
    """
    def __init__(self, options, settings):
        """
        options is command line start up args.
        settings is application settings loaded by settings.py or produced by settings.yaml
        root is ROOT folder of application
        """
        self.options = options
        self.root = settings.get("root", None)
        self._session_plugin_ = True
        self._error_plugin_ = True
        self.debug = settings.site.debug

        settings["title"] = settings.site.title
        settings["template_path"] = os.path.join(self.root, "app", "views")
        settings["static_path"] = os.path.join(self.root, "static")
        settings["xsrf_cookies"] = settings.site.xsrf_cookies
        settings["cookie_secret"] = settings.session.cookie_secret
        settings["login_url"] = settings.site.login_url
        settings["debug"] = settings.site.debug

        self.settings = settings
        self.initenvs()

        settings["ui_modules"] = self.ui_modules_map
        tornado.web.Application.__init__(
            self, handlers=self.handlers, **settings)
        # keep settings as ODict
        # logger.info("settings: %s" % self.settings)
        self.settings = settings

    def __call__(self, request):
        """
        Called by HTTPServer to execute the request.
        """
        return tornado.web.Application.__call__(self, request)

    def initenvs(self):
        """
        prepare running enviro
        """
        self._load_plugins()
        self._load_handlers()
        self._load_models()
        self._load_uimodules()

    def _load_handlers(self):
        """
        scan app/controllers folder to generate handlers and routes
        """
        logger.info("loading http request handlers")
        temp = __import__('app.controllers', globals(), locals(), [
                          'controllers'], -1)
        for name in [x for x in dir(temp) if re.findall('[A-Z]\w+', x)]:
            thing = getattr(temp, name)
            logger.info('%s, %s' % (name, thing))
            try:
                if issubclass(thing, Controller):
                    pass  # route.add(thing)
            except TypeError:
                # most likely a builtin class or something
                pass
        self.handlers = route.get_routes()
        self.handlers.append(tornado.web.url('/_debug/routes', UrlDebug))

    def _load_models(self):
        """
        scan app/models folder to generate models
        at handler can access Model like this:
        self.application.models.Person
        self.application.models.User
        """
        self.models = ODict({})
        temp = __import__('app.models', globals(), locals(), ['models'], -1)
        for name in [x for x in dir(temp) if re.findall('[A-Z]\w+', x)]:
            thing = getattr(temp, name)
            logger.info('%s, %s' % (name, thing))
            try:
                if issubclass(thing, EntityModel):
                    thing.init(self)
                    self.models[name] = thing
                elif isinstance(thing, enum):
                    self.models[name] = thing
            except TypeError:
                # most likely a builtin class or something
                pass
        logger.info(self.models)

    def _load_plugins(self):
        """
        load config plugins
        get plugin by name:
        1. self.plugins.session
        2. self.application.plugins.session
        """
        logger.info('loading plugins')
        enabled_plugins = []
        __import__('roo.plugins', globals(), locals(), ['defaultPlugins'], -1)
        __import__('app.plugins', globals(), locals(), ['plugins'], -1)
        if getattr(self, '_session_plugin_', None):
            enabled_plugins.append(pm.session(self))
        enabled_plugins.append(pm.email(self))
        for name in self.settings.plugins:
            thing = pm.get(name)
            if thing:
                enabled_plugins.append(thing(self))
        if getattr(self, '_error_plugin_', None):
            enabled_plugins.append(pm.error(self))
        self.enabled_plugins = enabled_plugins
        temp = ODict({})
        for item in enabled_plugins:
            temp[item.name] = item
        self.plugins = temp
        logger.info('loading plugins, %s' % self.plugins)

    def _load_uimodules(self):
        """
        load tornado ui modules
        """
        logger.info('loading ui modeuls')
        self.ui_modules_map = {}
        _ui_modules = __import__(
            'app.views.modules', globals(), locals(), ['ui_modules'], -1)
        for name in [x for x in dir(_ui_modules) if re.findall('[A-Z]\w+', x)]:
            thing = getattr(_ui_modules, name)
            logger.info(thing)
            try:
                if issubclass(thing, tornado.web.UIModule):
                    self.ui_modules_map[name] = thing
            except TypeError:
                # most likely a builtin class or something
                pass

    def start(self):
        """
        start server
        """
        pass


class RestApplication(RooApplication):

    """
    Support PUT、POST、GET、DELETE
    """
    def start(self):
        self.http_server = tornado.httpserver.HTTPServer(self, xheaders=True)
        print datetime.now(), "Starting tornado on port", self.options.port
        if self.options.prefork:
            print "\tpre-forking"
            self.http_server.bind(self.options.port)
            self.http_server.start()
        else:
            self.http_server.listen(self.options.port)
        logger.info('Starting at %s' % self.root)
        signal.signal(signal.SIGTERM, self.sig_handler)
        signal.signal(signal.SIGINT, self.sig_handler)
        try:
            tornado.ioloop.IOLoop.instance().start()
        except KeyboardInterrupt:
            pass

    def sig_handler(self, sig, frame):
        logger.warning('Caught signal: %s', sig)
        tornado.ioloop.IOLoop.instance().add_callback(self.shutdown)

    def shutdown(self):
        logger.info('Stopping http server')
        self.http_server.stop()

        logger.info('Will shutdown in %s seconds ...', SHUTDOWN_WAIT)
        io_loop = tornado.ioloop.IOLoop.instance()

        deadline = time.time() + SHUTDOWN_WAIT

        def stop_loop():
            now = time.time()
            if now < deadline and (io_loop._callbacks or io_loop._timeouts):
                io_loop.add_timeout(now + 1, stop_loop)
            else:
                io_loop.stop()
            logger.info('Shutdown')
        stop_loop()


class JobsApplication(RooApplication):

    """
    support ruuning task in background
    """
    def __init__(self, options, settings):
        self._session_plugin_ = False
        self._error_plugin_ = False
        self.cache_handlers = {}
        RooApplication.__init__(self, options, settings)

    def _load_handlers(self):
        """
        scan app/jobs folder to generate handlers and routes
        """
        logger.info("loading jobs message handlers")
        __import__('app.jobs', globals(), locals(), ['jobs'], -1)
        self.handlers = route.get_routes()

    def start(self):
        name = self.options.job
        engine = self.settings.jobengine(self, name)
        engine.start()

    def finish(self, chunk=None):
        self._finished = True
        self.on_finish()

    def make_request(self, path):
        method = path.split('/')[-1]
        headers = {}
        request = tornado.httpserver.HTTPRequest(
            method, path, headers=headers, remote_ip='127.0.0.1')
        return request

    def find_handler(self, request, path):
        if path in self.cache_handlers:
            return self.cache_handlers.get(path)
        args = []
        kwargs = {}
        handlers = self._get_host_handlers(request)
        # print handlers
        for spec in handlers:
            match = spec.regex.match(path)
            if match:
                handler = spec.handler_class(self, request, **spec.kwargs)
                if spec.regex.groups:
                    def unquote(s):
                        if s is None:
                            return s
                        return escape.url_unescape(s, encoding=None)
                    if spec.regex.groupindex:
                        kwargs = dict((str(k), unquote(v)) for (
                            k, v) in match.groupdict().items())
                    else:
                        args = [unquote(s) for s in match.groups()]
                break
        if not handler:
            raise Exception("Can't find handler: %s" % path)
        self.cache_handlers[path] = (handler, args, kwargs)
        return handler, args, kwargs

    def handle_message(self, message):
        """Called by HTTPServer to execute the request."""
        path = message.handler_url
        request = self.make_request(path)
        transforms = []
        handler, args, kwargs = self.find_handler(request, path)
        # In debug mode, re-compile templates and reload static files on every
        # request so you don't need to restart to see changes
        if self.settings.get("debug"):
            with Controller._template_loader_lock:
                for loader in Controller._template_loaders.values():
                    loader.reset()
        message.update(kwargs)
        handler._execute(transforms, *args, **message)
