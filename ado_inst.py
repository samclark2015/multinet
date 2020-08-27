from . import Multirequest
from itertools import groupby
from operator import itemgetter

io = Multirequest()


class AdoProperty:
    def on_update(self):
        print(self)


class AdoParameter:
    def __init__(self, name):
        self._name = name

    def __get__(self, obj, obj_type=None):
        return obj.get(self._name)

    def __set__(self, obj, value):
        obj.set(self._name, value)


class AdoInstance:
    def __new__(cls, name):
        cls_attrs = dict(_name=name)
        meta = io.get_meta((name,))[
            name,
        ]
        meta_dict = dict()
        for param, group in groupby(meta.keys(), itemgetter(0)):
            meta_dict[param] = {ent[-1]: meta[ent] for ent in group}
            cls_attrs[param] = AdoParameter(param)
        cls_attrs["_meta"] = meta_dict
        ado_cls = type(name + "Ado", (AdoInstance,), cls_attrs)
        return super().__new__(ado_cls)

    def get(self, param):
        keys = [(self._name, param, prop) for prop in self._meta[param].keys()]
        data = {key[-1]: val for key, val in io.get(*keys).items()}
        value = data["value"]
        parameter_type = type(
            str(type(value)) + "AdoParameter", (type(value), AdoParameter), {}
        )
        parameter = parameter_type(value)
        for key, val in data.items():
            property_type = type(
                str(type(val)) + "AdoProperty", (type(val), AdoProperty), {}
            )
            property_ = property_type(val)
            setattr(parameter, key, property_)
        return value

    def set(self, param, value, prop="value"):
        key = (self._name, param, prop, value)
        io.set(key)


if __name__ == "__main__":
    ado = AdoInstance("simple.test")
    print(ado.intS)
