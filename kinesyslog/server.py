import logging
import ssl
from asyncio import get_event_loop

from .protocol import (DatagramGelfProtocol, DatagramSyslogProtocol,
                       DefaultProtocol, GelfProtocol, SecureGelfProtocol,
                       SecureSyslogProtocol, SyslogProtocol)

logger = logging.getLogger(__name__)


class BaseServer(object):
    __slots__ = ['_host', '_port', '_loop', '_args']
    PROTOCOL = DefaultProtocol

    def __init__(self, host, port):
        logger.info('Starting {0} on {1}:{2}'.format(self.__class__.__name__, host, port))
        self._host = host
        self._port = port
        self._loop = get_event_loop()
        self._args = {}

    def _protocol_factory(self, sink):
        return lambda: self.PROTOCOL(sink=sink, **self._args)

    async def start_server(self, sink):
        return await self._loop.create_server(self._protocol_factory(sink), self._host, self._port)


class SecureServer(BaseServer):
    def __init__(self, certfile, keyfile, password=None, *args, **kwargs):
        super(SecureServer, self).__init__(*args, **kwargs)
        logger.info('{0} using cert {1} and key {2}'.format(self.__class__.__name__, certfile, keyfile))
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS)
        ctx.load_cert_chain(certfile, keyfile, password)
        self._args['sslcontext'] = ctx


class DatagramServer(BaseServer):
    async def start_server(self, sink):
        return await self._loop.create_datagram_endpoint(self._protocol_factory(sink), local_addr=(self._host, self._port))


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
