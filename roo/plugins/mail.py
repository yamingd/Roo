# -*- coding: utf-8 -*-
import roo.log
logger = roo.log.logger(__name__)

from roo.plugin import BasePlugin, plugin


@plugin
class EmailPlugin(BasePlugin):
    name = 'email'

    def __init__(self, application):
        BasePlugin.__init__(self, application)
        from roo.mail import build_engine
        from roo.config import settings
        if 'mail' in application.settings:
            self.engine = build_engine(application.settings.mail.engine)
            application.settings.mail.engine = self.engine
            settings.mail.engine = self.engine
