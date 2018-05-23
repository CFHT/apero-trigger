from collections import namedtuple
from .matchers import Matcher

ExposureConfig = namedtuple('ExposureConfig', 'calibwh rhomb1 rhomb2 refout cassout nexp exptime')


class ConfigCommandMap(object):
    def __init__(self):
        self.data = []

    def add(self, template, command):
        # check for no conflict TODO
        self.data.append((template, command))

    def get(self, configuration):
        for matcher, command in self.data:
            if matcher.matches(configuration):
                return command
        raise UnknownConfigError


class UnknownConfigError(Exception):
    pass


class ExposureTemplate:
    def __init__(self, config):
        self.config = config

    @classmethod
    def of(cls, calibwh, rhomb1, rhomb2, refout, cassout, nexp, exptime):
        config = ExposureConfig(
            calibwh = Matcher.parse(calibwh),
            rhomb1 = Matcher.parse(rhomb1),
            rhomb2 = Matcher.parse(rhomb2),
            refout = Matcher.parse(refout),
            cassout = Matcher.parse(cassout),
            nexp = Matcher.parse(nexp),
            exptime = Matcher.parse(exptime))
        return cls(config)

    def matches(self, exposure_values):
        for matcher, value in zip(self.config, exposure_values):
            if not matcher.match(value):
                return False
        return True

    def copy_and_set(self, **kwargs):
        matcher_kwargs = {key: Matcher.parse(kwargs[key]) for key in kwargs}
        return ExposureTemplate(self.config._replace(**matcher_kwargs))