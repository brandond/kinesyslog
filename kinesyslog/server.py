import logging
import ssl
from asyncio import ensure_future, get_event_loop
from asyncio.sslproto import SSLProtocol

logger = logging.getLogger(__name__)

LESSTHAN = 0x3C
DIGITS = bytearray(i for i in range(0x30, 0x3A))
TERMS = bytearray([0x00, 0x0A, 0x0D])
MAX_MESSAGE_LENGTH = 1024 * 16


class SyslogProtocol(object):
    __slots__ = ['sink', 'length', 'buffer', 'transport']

    def __init__(self, sink):
        self.sink = sink
        self.length = 0
        self.buffer = bytearray()
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        ensure_future(self._process_data(data))

    def eof_received(self):
        pass

    def error_received(self, exc):
        self._close_with_error('Protocol error: {0}'.format(exc))

    def connection_lost(self, exc):
        pass

    def pause_writing(self):
        pass

    def resume_writing(self):
        pass

    async def _process_data(self, data):
        self.buffer.extend(data)

        while self.buffer:
            message = None

            if self.length or self.buffer[0] in DIGITS:
                message = self._get_octet_counted_message()
            elif self.buffer[0] in TERMS:
                del self.buffer[0]
            elif self.buffer[0] == LESSTHAN:
                message = self._get_non_transparent_framed_message()
            else:
                self._close_with_error('Unable to determine framing for message: {0}'.format(self.buffer))

            if message:
                await self.sink.write(bytes(message))
            else:
                return

    def _close_with_error(self, message=None):
        if message:
            logger.error(message)
        if self.transport:
            self.transport.close()
        if self.buffer:
            self.buffer.clear()

    def _get_octet_counted_message(self):
        """
        Handle octet-counted messages by reading a space-separated length prefix,
        followed by the specified number of bytes. If insufficient bytes have been
        accumulated, remember the length and try again when we get more data.
        https://tools.ietf.org/html/rfc6587#section-3.4.1
        https://tools.ietf.org/html/rfc5425#section-4.3.1
        """
        if not self.length:
            length, sep, remainder = self.buffer.partition(b' ')
            if sep:
                try:
                    self.buffer = remainder
                    self.length = int(length)
                    if self.length < 0 or self.length > MAX_MESSAGE_LENGTH:
                        raise ValueError
                except ValueError:
                    self._close_with_error('Invalid MSG-LEN {0} for octet-counted message'.format(length))
                    return
            else:
                return

        if len(self.buffer) >= self.length:
            message = self.buffer[:self.length]
            del self.buffer[:self.length]
            self.length = 0
            return message
        else:
            return

    def _get_non_transparent_framed_message(self):
        """
        Handle non-transparently-framed messages by searching for one of several possible terminators
        https://tools.ietf.org/html/rfc6587#section-3.4.2
        """
        for separator in TERMS:
            message, sep, remainder = self.buffer.partition(bytes([separator]))
            if sep:
                if len(message) > MAX_MESSAGE_LENGTH:
                    message = message[:MAX_MESSAGE_LENGTH]
                self.buffer = remainder
                return message


class SecureSyslogProtocol(SSLProtocol):
    def __init__(self, certfile, keyfile, password=None, *args, **kwargs):
        loop = get_event_loop()
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS)
        ctx.load_cert_chain(certfile, keyfile, password)
        super(SecureSyslogProtocol, self).__init__(
            loop=loop,
            app_protocol=SyslogProtocol(*args, **kwargs),
            sslcontext=ctx,
            waiter=None,
            server_side=True
        )


class DatagramSyslogProtocol(SyslogProtocol):
    def connection_made(self, transport):
        # Don't store UDP transport to avoid closing it on parse failure
        pass

    def datagram_received(self, data, addr):
        # Append terminator to simplify parsing
        self.data_received(data + b'\x00')


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
        self.args['certfile'] = certfile
        self.args['keyfile'] = keyfile
        self.args['password'] = password


class DatagramSyslogServer(SyslogServer):
    PROTOCOL = DatagramSyslogProtocol

    async def start_server(self, sink):
        return await self.loop.create_datagram_endpoint(self._protocol_factory(sink), local_addr=(self.host, self.port))


# https://github.com/python/cpython/pull/480
def patch_pbo_29743():
    def _start_shutdown(self):
        if self._in_shutdown:
            return
        if self._in_handshake:
            self._abort()
        else:
            self._in_shutdown = True
            self._write_appdata(b'')

    SSLProtocol._start_shutdown = _start_shutdown


patch_pbo_29743()
