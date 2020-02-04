from abc import ABC
import json


class PluginParameter(ABC):
    def get_key(self):
        raise NotImplementedError()

    def get_type(self):
        raise NotImplementedError()

    def get_default_value(self):
        raise NotImplementedError()

    def get_required(self):
        raise NotImplementedError()


class IntPluginParameter(PluginParameter):
    def __init__(self, key, required=False, default_value=None):
        self.key = key
        self.required = required
        self.default_value = default_value

    def get_type(self):
        return 'int'

    def get_key(self):
        return self.key

    def get_default_value(self):
        return self.default_value

    def get_required(self):
        return self.required


class BooleanPluginParameter(PluginParameter):
    def __init__(self, key, required=False, default_value=None):
        self.key = key
        self.required = required
        self.default_value = default_value

    def get_type(self):
        return 'bool'

    def get_key(self):
        return self.key

    def get_default_value(self):
        return self.default_value

    def get_required(self):
        return self.required


class SpiderPlugin(ABC):
    parameters = []
    plugin_name = None

    def desc(self):
        ret_dict = {
            'name': self.plugin_name,
            'parameters': {},
        }
        for parameter in self.parameters:
            ret_dict['parameters'][parameter.get_key()] = {
                'type': parameter.get_type(),
                'required': parameter.get_required(),
                'default_value': parameter.get_default_value(),
            }
        return ret_dict

    def execute(self, parameters):
        pass

    def validate(self, parameters):
        pass