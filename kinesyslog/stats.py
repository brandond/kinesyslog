from .server import BaseServer


class StatsRegistry(object):
    """A dummy Prometheus Registry for use when Prometheus is not installed or not in use"""

    active = False

    def register(self, collector):
        pass

    def deregister(self, name):
        pass

    def get(self, name):
        raise KeyError()

    def get_all(self):
        return list()


class StatsSink(object):
    """A dummy sink that raises an error when used"""
    def __init__(self, *args, **kwargs):
        raise ImportError('Prometheus libraries not found - run "pip install kinesyslog[prometheus]"')


class StatsServer(BaseServer):
    """A dummy server that does nothing"""
    pass
