# -*- coding: utf-8 -*-
from roo import log
logger = log.logger(__name__)

from datetime import datetime
from roo.router import route

from .base import AdminBaseController


class SystemIndex(AdminBaseController):

    def get(self, *args, **kwargs):
        # self.write(kwargs)
        # self.write(self.__class__.__module__ +':' + self.__class__.__name__)
        self.xrender()


@route('/admin/system/solr/(?P<action>[a-zA-Z]+)')
class SystemSolr(AdminBaseController):

    def index(self, *args, **kwargs):
        # self.write(kwargs)
        # self.write(self.__class__.__module__ +':' + self.__class__.__name__)
        self.xrender()
