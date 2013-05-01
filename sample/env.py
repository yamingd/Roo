# -*- coding: utf-8 -*-
import sys
import os

import tornado.options
from tornado.options import define, options

from roo.lib import importlib
from roo.lib import ODict
from roo import log

define("debug", default=False, help="run in debug mode", type=bool)
define("port", default=8000, help="run on the given port", type=int)
define("prefork", default=False, help="pre-fork across all CPUs", type=bool)
define("job", default=None, help="Run job server", type=str)


def setup(args, settings_mod):
	print 'setup environ'
	args.append("-logging=none")
	tornado.options.parse_command_line(args)

	if '__init__.py' in settings_mod.__file__:
		p = os.path.dirname(settings_mod.__file__)
	else:
		p = settings_mod.__file__
	project_directory, settings_filename = os.path.split(p)
	if project_directory == os.curdir or not project_directory:
		project_directory = os.getcwd()
	project_name = os.path.basename(project_directory)

	# Strip filename suffix to get the module name.
	settings_name = os.path.splitext(settings_filename)[0]

	# Strip $py for Jython compiled files (like settings$py.class)
	if settings_name.endswith("$py"):
		settings_name = settings_name[:-3]

	# Set _SETTINGS_MODULE appropriately.
	# If _SETTINGS_MODULE is already set, use it.
	os.environ['_SETTINGS_MODULE'] = os.environ.get('_SETTINGS_MODULE', '%s.%s' % (project_name, settings_name))

	print project_name, project_directory
	# Import the project module. We add the parent directory to PYTHONPATH to
	# avoid some of the path errors new users can have.
	sys.path.append(os.path.join(project_directory, os.pardir))
	importlib.import_module(project_name)
	sys.path.pop()

	#logging
	log_folder = os.path.join(settings_mod.root, "logs")

	log.init(log_folder, options.port, settings_mod.site.logging)

	settings_map = ODict({})
	for name in dir(settings_mod):
		settings_map[name] = getattr(settings_mod, name)
	settings_map['root'] = project_directory
	return options, settings_map
