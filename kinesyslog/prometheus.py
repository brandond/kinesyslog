import logging

from aiohttp.hdrs import METH_GET as GET
from aiohttp.web import Application, RequestHandler, middleware
from aioprometheus.collectors import Counter, Gauge, Histogram
from aioprometheus.registry import CollectorRegistry
from aioprometheus.service import DEFAULT_METRICS_PATH, Service

from . import constant
from .server import BaseServer

logger = logging.getLogger(__name__)


class StatsSink(object):
    def __init__(self, spool, server, message_class, group_prefix, account):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass


class StatsRegistry(CollectorRegistry):
    active = True

    def register_collectors(self):
        self.register(Counter(name=constant.STAT_HTTP_REQS, doc='Total HTTP requests'))
        self.register(Counter(name=constant.STAT_MESSAGE_BYTES, doc='Message bytes received'))
        self.register(Counter(name=constant.STAT_MESSAGE_COUNT, doc='Message records received'))
        self.register(Counter(name=constant.STAT_BATCH_FAILED, doc='Kinesis batch record failures'))
        self.register(Gauge(name=constant.STAT_LISTENERS, doc='The number of message listeners'))
        self.register(Gauge(name=constant.STAT_SPOOL_AGE, doc='Kinesis batch spool record age'))
        self.register(Gauge(name=constant.STAT_SPOOL_COUNT, doc='Kinesis batch spool record count'))
        self.register(Histogram(name=constant.STAT_BATCH_RECORDS, doc='Kinesis batch record count',
                                buckets=[x for x in range(0, constant.MAX_BATCH_COUNT + 1, constant.MAX_BATCH_COUNT // 10)]))
        self.register(Histogram(name=constant.STAT_BATCH_BYTES, doc='Kinesis batch request size',
                                buckets=[x for x in range(0, constant.MAX_BATCH_SIZE + 1, constant.MAX_BATCH_SIZE // 8)]))
        self.register(Histogram(name=constant.STAT_RECORD_BYTES, doc='Kinesis record size',
                                buckets=[x for x in range(0, constant.MAX_RECORD_SIZE + 1, constant.MAX_RECORD_SIZE // 8)]))


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
    def __init__(self, sink, loop, registry, app):
        super(PrometheusHttpProtocol, self).__init__(loop=loop, manager=app._make_handler(loop=loop))


class StatsServer(BaseServer):
    PROTOCOL = PrometheusHttpProtocol

    def __init__(self, *args, **kwargs):
        super(StatsServer, self).__init__(*args, **kwargs)

        @middleware
        async def prometheus_middleware(request, handler):
            if self._registry.active:
                labels = {'method': request.match_info.route.method}
                if request.match_info.route.resource:
                    labels['path'] = request.match_info.route.resource.canonical
                self._registry.get(constant.STAT_HTTP_REQS).inc(labels=labels)
            return await handler(request)

        self._args['app'] = Application(middlewares=[prometheus_middleware])
        StatsService(self._args['app'], registry=self._registry)
