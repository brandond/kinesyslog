from .server import BaseServer


class StatsRegistry(object):
    active = False

    def register(self, collector):
        pass

    def deregister(self, name):
        pass

    def get(self, name):
        raise KeyError()

    def get_all(self):
        return list()

    def register_collectors(self):
        pass


class StatsSink(object):
    def __init__(self, *args, **kwargs):
        raise ImportError('Prometheus libraries not found - run "pip install kinesyslog[prometheus]"')


class StatsServer(BaseServer):
    pass
