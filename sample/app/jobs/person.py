# -*- coding: utf-8 -*-
from roo import log
logger = log.logger(__name__)

from roo.job import JobMessageHandler
from roo.router import route


@route("/jobs/person/(?P<action>[a-zA-Z]+)")
class Person(JobMessageHandler):
	
	def index(self, *args, **kwargs):
		logger.info(args)
		logger.info(kwargs)
		pass

	def add(self, *args, **kwargs):
		logger.info(args)
		logger.info(kwargs)

	