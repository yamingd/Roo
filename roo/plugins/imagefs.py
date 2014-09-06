#!/usr/bin/env python
# -*- coding: utf-8 -*-

# -*- coding: utf-8 -*-
import roo.log
logger = roo.log.logger(__name__)

from roo.plugin import BasePlugin, plugin


@plugin
class ImageFSPlugin(BasePlugin):
    name = 'imageFS'

    def __init__(self, application):
        BasePlugin.__init__(self, application)
        from roo.image import localfs
        if 'image' in application.settings:
            self.fs = localfs.LocalImageFS()
        else:
            self.fs = None
        setattr(application, 'imagefs', self.fs)
        
    def on_before(self, controller):
        setattr(controller, 'imagefs', self.fs)
