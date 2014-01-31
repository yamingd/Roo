# -*- coding: utf-8 -*-

from roo.validation import validators_map


class Field(object):

    def __init__(self, *args):
        self.vmap = args
    
    def parse(self, value):
        return value


class TextField(Field):

    def __init__(self, *args):
        self.vmap = args
    
    def parse(self, value):
        return value


class NumberField(Field):

    def __init__(self, *args):
        self.vmap = args
    
    def parse(self, value):
        if value and len(value) > 0:
            if ',' in value:
                return map(long, value.split(','))
            return long(value)
        return None


class Form(object):

    def __init__(self, controller, model, field_prefix=None):
        self.errors = {}
        self.c = controller
        self.request = controller.request
        self.model = model.__name__.lower() if hasattr(model, '__name__') else model.lower()
        self.field_prefix = field_prefix

    def validate(self):
        """
        validate current post form's data
        """
        items = self.__class__.__dict__
        for item in items:
            field = items.get(item)
            if isinstance(field, Field):
                keyname = item if self.field_prefix is None else self.field_prefix + '.' + item
                values = self.c.get_arguments(keyname)
                if field.vmap is None:
                    continue
                for vname, vargs in field.vmap:
                    vc = validators_map.get(vname, None)
                    if not vc:
                        raise Exception(
                            "can't find validator with name: %s" % vname)
                    vv = vc(**vargs)
                    # print vv, values
                    if not vv.isOK(values):
                        self.errors[keyname] = vv.get_message()
                        break
        return self.errors
    
    def get_model(self):
        """
        create model instances from request.
        """
        m = {}
        items = self.__class__.__dict__
        for key in items:
            k2 = key if self.field_prefix is None else self.field_prefix + '.' + key
            field = items.get(key)
            if not isinstance(field, Field):
                continue
            values = self.c.get_arguments(k2)
            if len(values) == 1:
                values = field.parse(values[0])
            elif len(values) < 1:
                values = field.parse('')
            else:
                values = field.parse(values)
            m[key] = values
        return m


class ATestForm(Form):
    name = Field(
                    ("required", {'msg': u"Your name is required."}),
                    ("minsize", {'msg': u"at least 5 charachters.",'min': 5})
            )
    email = Field(
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
