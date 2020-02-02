from abc import ABC


class SpiderPlugin(ABC):
    def desc(self):
        pass

    def execute(self, parameters):
        pass

    def validate(self, parameters):
        pass