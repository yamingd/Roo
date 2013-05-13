# -*- coding: utf-8 -*-

from roo.validation import validators_map


class Field(object):

    def __init__(self, *args):
        self.vmap = args


class Form(object):

    def __init__(self, controller, model):
        self.errors = {}
        self.c = controller
        self.request = controller.request
        self.model = model.__class__.__name__.lower()

    def validate(self):
        """
        validate current post form's data
        """
        items = self.__class__.__dict__
        for item in items:
            field = items.get(item)
            if isinstance(field, Field):
                keyname = self.model + '.' + item
                values = self.c.get_arguments(keyname)
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
        if self.errors:
            self.errors['status'] = '601'
        return self.errors

    def get_models(self):
        """
        create model instances from request.
        """
        m = {}
        args = self.request.arguments
        for key in args:
            tmp = key.split('.')
            if len(tmp) == 1:
                continue
            m.setdefault(tmp[0], {})
            m[tmp[0]][tmp[1]] = self.c.get_argument(key)
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
