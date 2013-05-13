# -*- coding: utf-8 -*-
import roo.log
logger = roo.log.logger(__name__)

import tornado.web
import threadlocal


class Controller(tornado.web.RequestHandler):
	require_auth_action = []
	require_auth = False

	def __init__(self, application, request, transforms=None):
		self.render_context = {}
		self.action_method = None
		tornado.web.RequestHandler.__init__(self, application, request)

	def get_req_format(self):
		accept = self.request.headers.get("accept", None)
		if not accept or len(accept) == 0:
			return "html"
		if accept.find("application/xhtml") >= 0 or accept.find("text/html") >= 0:
			return "html"
		if accept.find("application/xml") >= 0 or accept.find("text/xml") >= 0:
			return "xml"
		if accept.find("text/plain") >= 0:
			return "txt"
		if accept.find("application/json") >= 0 or accept.find("text/javascript") >= 0:
			return "json"
		return "html"

	def get_template_file(self):
		"""
		located by controller, method and output format.
		"""
		folder = self.__class__.__name__.lower()
		folder = folder.replace('controller', '').replace('handler', '')
		tmpl_name = self.action_method.lower()
		format = self.get_req_format()
		return "%s/%s.%s" % (folder, tmpl_name, format)

	@property
	def is_xhr(self):
		h = self.request.headers.get('X-Requested-With', None)
		return h and h == 'XMLHttpRequest'
	
	@property
	def models(self):
		return self.application.models

	@property
	def config(self):
		return self.application.settings

	def add_render_args(self, **kwargs):
		"""
		add render data args
		"""
		self.render_context.update(kwargs)

	def get_template_namespace(self):
		"""
		override to prepare template rending datas.
		"""
		kwargs = tornado.web.RequestHandler.get_template_namespace(self)
		kwargs.update(self.render_context)
		kwargs['conf'] = self.application.settings
		return kwargs

	def xrender(self, **kwargs):
		"""
		kwargs is view data
		output html / json / xml content.
		use this instead of either render or render_string
		"""
		template_name = self.get_template_file()
		logger.debug('xrender: %s' % template_name)
		return tornado.web.RequestHandler.render(self, template_name, **kwargs)
	
	def prepare(self):
		"""
		Called at the beginning of a request before `get`/`post`/etc.
		Override this method to perform common initialization regardless
		of the request method.
		"""
		for plugin in self.enabled_plugins:
			plugin.on_before(self)

	@property
	def enabled_plugins(self):
		return self.application.enabled_plugins

	def on_finish(self):
		"""Called after the end of a request.

		Override this method to perform cleanup, logging, etc.
		This method is a counterpart to `prepare`.  ``on_finish`` may
		not produce any output, as it is called after the response
		has been sent to the client.
		"""
		for plugin in self.enabled_plugins:
			plugin.on_finished(self)

	def _handle_request_exception(self, e):
		"""
		override to handle Exception
		"""
		for plugin in self.enabled_plugins:
			plugin.on_exception(self, e)
		tornado.web.RequestHandler._handle_request_exception(self, e)

	def get_current_user(self):
		"""
		Override to determine the current user from, e.g., a cookie.
		use SessionPlugin to init current user from cookie and then save to threadlocal
		"""
		if not hasattr(self, '_current_user'):
			self._current_user = threadlocal.get_user()
		return self._current_user

	def remember_user(self, user, remember_me):
		"""
		set current user and store user-id to cookie
		"""
		threadlocal.set_user(user)
		session_config = self.config.session
		cookie_id = str(session_config.auth_cookie)
		if user:
			self._current_user = user
			if remember_me:
				self.set_secure_cookie(cookie_id, str(user.id), expires_days=session_config.expires_days)
			else:
				self.set_secure_cookie(cookie_id, str(user.id), expires_days=None)
		else:
			self.set_secure_cookie(cookie_id, str(-1), expires_days=None)

	def clear_user(self):
		"""
		called on user sign out.
		"""
		session_config = self.config.session
		cookie_id = str(session_config.auth_cookie)
		self.clear_cookie(cookie_id)
	
	def abort(self, code='501', msg='abort'):
		"""
		abort this request as security reason or validation.
		"""
		raise tornado.web.HTTPError(code, msg)

	def _execute(self, transforms, *args, **kwargs):
		"""Executes this request with the given output transforms."""
		self._transforms = transforms
		try:
			if self.request.method not in self.SUPPORTED_METHODS:
				raise tornado.web.HTTPError(405)
			self.path_args = [self.decode_argument(arg) for arg in args]
			self.path_kwargs = dict((k, self.decode_argument(v, name=k))
			                        for (k, v) in kwargs.items())
			# If XSRF cookies are turned on, reject form submissions without
			# the proper cookie
			if self.request.method not in ("GET", "HEAD", "OPTIONS") and \
					self.application.settings.get("xsrf_cookies"):
				self.check_xsrf_cookie()
			self.prepare()
			if not self._finished:
				self._execute_method(*self.path_args, **self.path_kwargs)
				if self._auto_finish and not self._finished:
					self.finish()
		except Exception as e:
			self._handle_request_exception(e)

	def _execute_method(self, *args, **kwargs):
		"""

		Routes as follows:
		A: /person/([a-zA-Z]+)/([0-9]+)
		B: /person/(?P<action>[a-zA-Z]+)/(?P<id>[0-9]+)

		Request as follows:
		/hello/123

		for A: args = ("hello","123"), kwargs = {}
		for B: args = (), kwargs = {"action":"hello", "id": "123"}

		"""
		method_name = self.request.method.lower()
		if 'action' in kwargs:
			method_name = kwargs['action']
			del kwargs['action']
		self.action_method = method_name
		func = getattr(self, method_name)
		func(*args, **kwargs)

