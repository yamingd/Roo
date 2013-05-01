# -*- coding: utf-8 -*-
import sys
import os


try:
	# Assumed to be in the same directory.
	import settings
except ImportError:
	sys.stderr.write("Error: Can't find the file 'settings.py' in the directory containing %r. It appears you've customized things.\n(If the file settings.py does indeed exist, it's causing an ImportError somehow.)\n" % __file__)
	sys.exit(1)


if __name__ == "__main__":
	args = sys.argv or []
	settings.root = os.path.dirname(__file__)

	import env
	options, settings_map = env.setup(args, settings)

	from roo import mvc
	if options.job:
		app = mvc.JobsApplication(options, settings_map)
	else:
		app = mvc.RestApplication(options, settings_map)
	app.start()
