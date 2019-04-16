import logging
import ssl
from asyncio.base_events import Server

from .protocol import (DatagramGelfProtocol, DatagramSyslogProtocol,
                       DefaultProtocol, GelfProtocol, SecureGelfProtocol,
                       SecureSyslogProtocol, SyslogProtocol)

logger = logging.getLogger(__name__)


class BaseServer(object):
    __slots__ = ['_host', '_port', '_args', '_server']
    PROTOCOL = DefaultProtocol

    def __init__(self, host, port, registry):
        logger.info('Starting {0} on {1}:{2}'.format(self.__class__.__name__, host, port))
        self._host = host
        self._port = port
        self._registry = registry
        self._args = {}

    def _protocol_factory(self, sink, loop):
        return lambda: self.PROTOCOL(sink=sink, loop=loop, registry=self._registry, **self._args)

    async def start(self, sink, loop):
        logger.debug('Starting server: {0}'.format(self.__class__.__name__))
        self._server = await loop.create_server(
            protocol_factory=self._protocol_factory(sink, loop),
            host=self._host,
            port=self._port,
            reuse_address=True,
            reuse_port=True)
        return self._server

    async def stop(self):
        logger.debug('Stopping server: {0}'.format(self.__class__.__name__))
        server = self._server
        if server is None:
            return
        self._server = None
        server.close()
        return await server.wait_closed()


class SecureServer(BaseServer):
    def __init__(self, certfile, keyfile, password=None, *args, **kwargs):
        super(SecureServer, self).__init__(*args, **kwargs)
        logger.info('{0} using cert {1} and key {2}'.format(self.__class__.__name__, certfile, keyfile))
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS)
        ctx.load_cert_chain(certfile, keyfile, password)
        self._args['sslcontext'] = ctx


class DatagramServer(BaseServer):
    async def start(self, sink, loop):
        logger.debug('Starting server: {0}'.format(self.__class__.__name__))
        transport, protocol = await loop.create_datagram_endpoint(
            protocol_factory=self._protocol_factory(sink, loop),
            local_addr=(self._host, self._port),
            reuse_address=True,
            reuse_port=True)
        self._transport = transport
        self._server = Server(loop, [transport._sock])
        return self._server

    async def stop(self):
        await super(DatagramServer, self).stop()
        if self._transport:
            self._transport.close()


class SyslogServer(BaseServer):
    PROTOCOL = SyslogProtocol


class SecureSyslogServer(SecureServer):
    PROTOCOL = SecureSyslogProtocol


class DatagramSyslogServer(DatagramServer):
    PROTOCOL = DatagramSyslogProtocol


class GelfServer(BaseServer):
    PROTOCOL = GelfProtocol


class SecureGelfServer(SecureServer):
    PROTOCOL = SecureGelfProtocol


class DatagramGelfServer(DatagramServer):
    PROTOCOL = DatagramGelfProtocol
