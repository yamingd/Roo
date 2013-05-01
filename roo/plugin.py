# -*- coding: utf-8 -*-
import roo.log
logger = roo.log.logger(__name__)


class BasePlugin(object):
    name = None
    """
    Plugin base class, all plugin should subclass from this.
    """
    def __init__(self, application):
        self.application = application
        self.debug = application.settings.site.debug

    def __repr__(self):
        return u"<Plugin %s>" % self.get_name()

    def get_name(self):
        """
        return plugin's name
        """
        return self.name

    def on_before(self, controller):
        """
        execute before send action to controller, you can init something you need later.
        """
        pass

    def on_exception(self, controller, e):
        """
        execute on exception
        """
        pass

    def on_finished(self, controller):
        """
        execute when action and exception finish handling
        """
        pass

    @property
    def config(self):
        return self.application.settings

cplugins = {}


class PluginManager(object):

    def __setitem__(self, name, value):
        if value is None and name in cplugins:
            cplugins.pop(name)
        cplugins[name] = value

    def __getitem__(self, name):
        if name in cplugins:
            cc = cplugins[name]
            return cc
        return None

    def __getattr__(self, name):
        try:
            return cplugins[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        cplugins[name] = value

    def get(self, name):
        if name in cplugins:
            return cplugins[name]
        return None

manager = PluginManager()


def plugin(clzz):
    manager[clzz.name] = clzz
    logger.debug('found plugin: %s' % clzz.name)
    return clzz
