# -*- coding: utf-8 -*-
import re


class Validator(object):
	defaultMessage = u''
	name = None

	def __init__(self, **kwargs):
		self.message = kwargs.get('msg', self.defaultMessage)
		self.kwargs = kwargs

	def isOK(self, val):
		"""
		val is a array from Html Form
		"""
		pass

	def get_message(self):
		if self.message:
			return self.message
		return self.defaultMessage


class Required(Validator):
	defaultMessage = u'Required'
	name = 'required'

	def isOK(self, val):
		if not val:
			return False
		val = val[0]
		if len(val) == 0:
			return False
		return True


class Min(Validator):
	defaultMessage = u'Minimum is {0}'
	name = 'min'

	def isOK(self, val):
		if not val:
			return True
		val = val[0]
		if int(val) <= self.kwargs['min']:
			return False
		return True
	
	def get_message(self):
		return self.message.format(self.kwargs['min'],)


class Max(Validator):
	defaultMessage = u'Maximum is {0}'
	name = 'max'

	def isOK(self, val):
		if not val:
			return True
		val = val[0]
		if int(val) >= self.kwargs['max']:
			return False
		return True
	
	def get_message(self):
		return self.message.format(self.kwargs['max'],)


class Range(Validator):
	"""
	Requires an integer to be within Min, Max inclusive.
	"""
	defaultMessage = u'Range is {0} to {1}'
	name = 'range'

	def isOK(self, val):
		if not val:
			return True
		val = val[0]
		if int(val) >= self.kwargs['max']:
			return False
		if int(val) <= self.kwargs['min']:
			return False
		return True
	
	def get_message(self):
		return self.message.format(self.kwargs['min'], self.kwargs['max'])


class MinSize(Validator):
	"""
	Requires an array or string to be at least a given length.
	"""
	defaultMessage = u'Minimum size is {0}'
	name = 'minsize'

	def isOK(self, val):
		if not val:
			return True
		val = val[0]
		if len(val) <= self.kwargs['min']:
			return False
		return True
	
	def get_message(self):
		return self.message.format(self.kwargs['min'],)


class MaxSize(Validator):
	"""
	Requires an array or string to be at most a given length.
	"""
	defaultMessage = u'Maximum size is {0}'
	name = 'maxsize'

	def isOK(self, val):
		if not val:
			return True
		val = val[0]
		if len(val) >= self.kwargs['max']:
			return False
		return True
	
	def get_message(self):
		return self.message.format(self.kwargs['max'],)


class Length(Validator):
	"""
	Requires an array or string to be exactly a given length.
	"""
	defaultMessage = u'Required length is {0}'
	name = 'length'

	def isOK(self, val):
		if not val:
			return True
		val = val[0]
		if len(val) == self.kwargs['len']:
			return False
		return True
	
	def get_message(self):
		return self.message.format(self.kwargs['len'],)


class Match(Validator):
	"""
	Requires an array or string to be exactly a given length.
	"""
	defaultMessage = u'Must match {0}'
	name = 'match'

	def __init__(self, **kwargs):
		self.message = kwargs.get('msg', self.defaultMessage)
		self.reg = re.compile(kwargs['regx'])
		self.kwargs = kwargs

	def isOK(self, val):
		if not val:
			return True
		val = val[0]
		match = self.reg.search(val)
		return match is not None
	
	def get_message(self):
		return self.message.format(self.kwargs.get('regx', ''),)


usernameRE = re.compile(r"^[^ \t\n\r@<>()]+$", re.I)
domainRE = re.compile(r'''
    ^(?:[a-z0-9][a-z0-9\-]{0,62}\.)+ # (sub)domain - alpha followed by 62max chars (63 total)
    [a-z]{2,}$                       # TLD
''', re.I | re.VERBOSE)


class EmailMatch(Match):
	defaultMessage = u'Must be a valid email address'
	name = 'email'

	def __init__(self, **kwargs):
		self.message = kwargs.get('msg', self.defaultMessage)
		self.kwargs = kwargs

	def isOK(self, value):
		if not value:
			return True
		value = value[0]
		value = value.strip()
		splitted = value.split('@', 1)
		try:
			username, domain = splitted
			if not usernameRE.search(username):
				return False
			if not domainRE.search(domain):
				return False
			return True
		except ValueError:
			return False

url_re = re.compile(r'''
        ^(http|https)://
        (?:[%:\w]*@)?                           # authenticator
        (?P<domain>[a-z0-9][a-z0-9\-]{1,62}\.)* # (sub)domain - alpha followed by 62max chars (63 total)
        (?P<tld>[a-z]{2,})                      # TLD
        (?::[0-9]+)?                            # port

        # files/delims/etc
        (?P<path>/[a-z0-9\-\._~:/\?#\[\]@!%\$&\'\(\)\*\+,;=]*)?
        $
    ''', re.I | re.VERBOSE)
url_scheme_re = re.compile(r'^[a-zA-Z]+:')


class UrlMatch(Match):
	defaultMessage = u'Must be a valid HTTP address'
	name = 'url'

	def __init__(self, **kwargs):
		self.message = kwargs.get('msg', self.defaultMessage)
		self.kwargs = kwargs

	def isOK(self, url):
		if not url:
			return True
		url = url[0]
		url = url.strip()
		if not url_scheme_re.search(url):
			url = 'http://' + url
		match = url_scheme_re.search(url)
		value = match.group(0).lower() + url[len(match.group(0)):]
		match = url_re.search(value)
		if not match:
			return False
		return True


