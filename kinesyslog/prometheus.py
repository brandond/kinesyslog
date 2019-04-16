import logging

from aioprometheus.service import Service, DEFAULT_METRICS_PATH
from aioprometheus.registry import CollectorRegistry
from aiohttp.web import RequestHandler, Application
from aiohttp.hdrs import METH_GET as GET

from .server import BaseServer

logger = logging.getLogger(__name__)


class StatsSink(object):
    def __init__(self, spool, server, message_class, group_prefix):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass


class StatsRegistry(CollectorRegistry):
    active = True


class StatsService(Service):
    def __init__(self, app, metrics_url=DEFAULT_METRICS_PATH, *args, **kwargs):
        super(StatsService, self).__init__(*args, **kwargs)
        self._app = app
        self._metrics_url = metrics_url
        self._app["metrics_url"] = metrics_url
        self._app.router.add_route(GET, metrics_url, self.handle_metrics)
        self._app.router.add_route(GET, self._root_url, self.handle_root)
        self._app.router.add_route(GET, "/robots.txt", self.handle_robots)


class PrometheusHttpProtocol(RequestHandler):
    def __init__(self, sink, loop, app):
        super(PrometheusHttpProtocol, self).__init__(loop=loop, manager=app._make_handler(loop=loop))


class StatsServer(BaseServer):
    PROTOCOL = PrometheusHttpProtocol

    def __init__(self, *args, **kwargs):
        super(StatsServer, self).__init__(*args, **kwargs)
        self._args['app'] = Application()
        StatsService(self._args['app'], registry=self._registry)
