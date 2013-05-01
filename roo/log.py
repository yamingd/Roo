# -*- coding: utf-8 -*-
import logging
import logging.handlers
import time


def logger(name):
	logger = logging.getLogger(name)
	return logger


def init(folder, port, level):
	"""
	Turns on formatted logging output as configured.
	options = ODict({'log_folder':'','runtime':'','port':''})
	"""
	file_prefix = "%s/%s.log" % (folder, port)
	file_size = 10 * 1000 * 1000
	file_backups = 30
	
	root_logger = logging.getLogger()
	root_logger.setLevel(getattr(logging, level.upper()))
	
	channel = logging.handlers.RotatingFileHandler(filename=file_prefix, maxBytes=file_size, backupCount=file_backups)
	channel.setFormatter(_LogFormatter())
	root_logger.addHandler(channel)


class _LogFormatter(logging.Formatter):
	def __init__(self, *args, **kwargs):
		logging.Formatter.__init__(self, *args, **kwargs)

	def format(self, record):
		try:
			record.message = record.getMessage()
		except Exception, e:
			record.message = "Bad message (%r): %r" % (e, record.__dict__)
		record.asctime = time.strftime("%Y-%m-%d %H:%M:%S", self.converter(record.created))
		prefix = '[%(asctime)s %(levelname)s %(name)s:%(lineno)d]' % record.__dict__
		formatted = prefix + " " + record.message
		if record.exc_info:
			if not record.exc_text:
				record.exc_text = self.formatException(record.exc_info)
		if record.exc_text:
			formatted = formatted.rstrip() + "\n" + record.exc_text
		return formatted.replace("\n", "\n    ")
