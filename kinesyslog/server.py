import logging
import ssl
from asyncio import get_event_loop

from .protocol import (DatagramGelfProtocol,
                       DatagramSyslogProtocol, GelfProtocol,
                       SecureGelfProtocol, SecureSyslogProtocol,
                       SyslogProtocol)

logger = logging.getLogger(__name__)


class SyslogServer(object):
    PROTOCOL = SyslogProtocol
    __slots__ = ['host', 'port', 'loop', 'args']

    def __init__(self, host, port):
        logger.info('Starting {0} on {1}:{2}'.format(self.__class__.__name__, host, port))
        self.host = host
        self.port = port
        self.loop = get_event_loop()
        self.args = {}

    def _protocol_factory(self, sink):
        return lambda: self.PROTOCOL(sink=sink, **self.args)

    async def start_server(self, sink):
        return await self.loop.create_server(self._protocol_factory(sink), self.host, self.port)


class SecureSyslogServer(SyslogServer):
    PROTOCOL = SecureSyslogProtocol

    def __init__(self, certfile, keyfile, password=None, *args, **kwargs):
        super(SecureSyslogServer, self).__init__(*args, **kwargs)
        logger.info('{0} using cert from {1} and key from {2}'.format(self.__class__.__name__, certfile, keyfile))
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
        ctx.load_cert_chain(certfile, keyfile, password)
        self.args['sslcontext'] = ctx


class DatagramSyslogServer(SyslogServer):
    PROTOCOL = DatagramSyslogProtocol

    async def start_server(self, sink):
        return await self.loop.create_datagram_endpoint(self._protocol_factory(sink), local_addr=(self.host, self.port))


class GelfServer(SyslogServer):
    PROTOCOL = GelfProtocol


class SecureGelfServer(SecureSyslogServer):
    PROTOCOL = SecureGelfProtocol


class DatagramGelfServer(DatagramSyslogServer):
    PROTOCOL = DatagramGelfProtocol
