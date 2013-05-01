# -*- coding: utf-8 -*-

__all__ = ["Form", "ValidationField", "addValidator"]

import validators

validators_map = {}


def init():
	items = validators.__dict__
	for name in items:
		if name.startswith('__'):
			continue
		clzz = getattr(validators, name)
		cname = getattr(clzz, 'name', None)
		if cname:
			validators_map[cname] = clzz
	#print validators_map


def addValidator(clzz):
	cname = getattr(clzz, 'name', None)
	if cname:
		validators_map[cname] = clzz


class Form(object):
	def __init__(self):
		self.errors = {}

	def validate(self, request):
		items = self.__class__.__dict__
		for item in items:
			field = items.get(item)
			if isinstance(field, ValidationField):
				values = request.get_arguments(item)
				for vname, vargs in field.vmap:
					vc = validators_map.get(vname, None)
					if not vc:
						raise Exception("can't find validator with name: %s" % vname)
					vv = vc(**vargs)
					#print vv, values
					if not vv.isOK(values):
						self.errors[item] = vv.get_message()
						break
		return len(self.errors) == 0


class ValidationField(object):
	def __init__(self, *args):
		self.vmap = args

init()


class ATestForm(Form):
	name = ValidationField(
			("required", {'msg': u"Your name is required."}),
			("minsize", {'msg': u"at least 5 charachters.",'min': 5})
		)
	email = ValidationField(
			("required", {'msg': u"Your email is required."}),
			("email", {'msg': u"Your email is not validated"})
		)


class FakeRequest(dict):
	def get_arguments(self, name):
		return self.get(name, None)


def testAForm():
	aform = ATestForm()
	request = FakeRequest({
		'name': [u'yamingd'],
		'email': [u'yamingd@gmail.com']
		})
	print aform.validate(request)
	print aform.errors

	request = FakeRequest({
		'name': [u'yamingd'],
		'email': [u'yamingd@gmail']
		})
	print aform.validate(request)
	print aform.errors

if __name__ == '__main__':
	testAForm()
