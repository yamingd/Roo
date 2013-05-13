# -*- coding: utf-8 -*-
import tornado.web

import roo.log
logger = roo.log.logger(__name__)


class Route(object):

    """
    decorates RequestHandlers and builds up a list of routables handlers

    Tech Notes (or 'What the *@# is really happening here?')
    --------------------------------------------------------

    Everytime @route('...') is called, we instantiate a new route object which
    saves off the passed in URI.  Then, since it's a decorator, the function is
    passed to the route.__call__ method as an argument.  We save a reference to
    that handler with our uri in our class level routes list then return that
    class to be instantiated as normal.

    Later, we can call the classmethod route.get_routes to return that list of
    tuples which can be handed directly to the tornado.web.Application
    instantiation.

    Example
    -------

    @route('/some/path')
    class SomeRequestHandler(RequestHandler):
        pass

    @route('/some/path', name='other')
    class SomeOtherRequestHandler(RequestHandler):
        pass

    my_routes = route.get_routes()
    """
    _routes = []

    def __init__(self, uri, name=None):
        self._uri = uri
        self.name = name

    def __call__(self, _handler):
        """gets called when we class decorate"""
        name = self.name and self.name or _handler.__name__
        setattr(_handler, '_uri', self._uri)
        self._routes.append(tornado.web.url(self._uri, _handler, name=name))
        return _handler

    @classmethod
    def get_routes(self):
        return self._routes

    @classmethod
    def add(self, handler):
        if getattr(handler, '_url', None):
            return
        ns = handler.__module__.lower()
        ns = ns.replace('app.controllers.', '')
        ns = ns.split('.')[0:-1]
        clzz = handler.__name__.replace(
            'Handler', '').replace('Controller', '')
        path = []
        path.extend(ns)
        for c in clzz:
            if c >= 'A' and c <= 'Z':
                path.append('/')
                path.append(c)
            else:
                path.append(c)
        path = ''.join(path).lower()
        if not path.startswith('/'):
            path = '/' + path
        self._routes.append(tornado.web.url(
            path, handler, name=handler.__name__))


def route_redirect(from_, to, name=None):
    route._routes.append(tornado.web.url(
        from_, tornado.web.RedirectHandler, dict(url=to), name=name))


route = Route
