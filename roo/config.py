# -*- coding: utf-8 -*-
import os

from roo.lib import importlib
from roo.lib.functional import LazyObject, empty

ENV_VARIABLE = "_SETTINGS_MODULE"


class LazySettings(LazyObject):
    """
    A lazy proxy for either global settings or a custom settings object.
    The user can manually configure settings prior to using them. Otherwise,
    uses the settings module pointed to by _SETTINGS_MODULE.
    """
    def _setup(self):
        """
        Load the settings module pointed to by the environment variable. This
        is used the first time we need any settings at all, if the user has not
        previously configured the settings manually.
        """
        try:
            settings_module = os.environ[ENV_VARIABLE]
            if not settings_module:   # If it's set but is an empty string.
                raise KeyError
        except KeyError:
            # NOTE: This is arguably an EnvironmentError, but that causes
            # problems with Python's interactive help.
            raise ImportError("Settings cannot be imported, environment variable %s is undefined." % ENV_VARIABLE)

        self._wrapped = Settings(settings_module)

    @property
    def configured(self):
        """
        Returns True if the settings have already been configured.
        """
        return self._wrapped is not empty


class BaseSettings(object):
    _keys = []
    """
    Common logic for settings whether set by a module or by the user.
    """
    def __setattr__(self, name, value):
        object.__setattr__(self, name, value)
        self._keys.append(name)
    
    @property
    def keys(self):
        return self._keys


class Settings(BaseSettings):
    exl_tokens = ['type', 'module']

    def __init__(self, settings_module):
        # store the settings module in case someone later cares
        self.SETTINGS_MODULE = settings_module

        try:
            mod = importlib.import_module(self.SETTINGS_MODULE)
        except ImportError, e:
            raise ImportError("Could not import settings '%s' (Is it on sys.path?): %s" % (self.SETTINGS_MODULE, e))

        for setting in dir(mod):
            if not setting.startswith('__'):
                setting_value = getattr(mod, setting)
                if setting_value.__class__.__name__ not in self.exl_tokens:
                    setattr(self, setting, setting_value)

settings = LazySettings()
