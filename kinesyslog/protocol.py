import gzip
import logging
import zlib
from asyncio import ensure_future, get_event_loop
from asyncio.sslproto import SSLProtocol

from . import constant
from .gelf import ChunkedMessage

logger = logging.getLogger(__name__)


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
        if self.transport and self.transport.is_closing():
            return

        self.buffer.extend(data)
        while self.buffer:
            message = None

            if self.length or self.buffer[0] in constant.DIGITS:
                message = self._get_octet_counted_message()
            elif self.buffer[0] in constant.TERMS:
                del self.buffer[0]
            elif self.buffer[0] == constant.LESSTHAN:
                message = self._get_non_transparent_framed_message()
            elif self.buffer[0] in constant.METHODS:
                self._close_with_http()
            else:
                self._close_with_error('{0} unable to determine framing for message: {1}'.format(self.__class__.__name__, self.buffer))

            if message:
                await self.sink.write(bytes(message))
            else:
                return

    def _close_with_error(self, message=None):
        if message:
            logger.error(message)
        if self.buffer:
            self.buffer.clear()
        if self.transport:
            self.transport.close()

    def _close_with_http(self):
        if self.buffer:
            self.buffer.clear()
        if self.transport:
            logger.debug('Sending HTTP response and closing connection')
            self.transport.write(b'HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n')
            self.transport.close()

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
                    if self.length < 0 or self.length > constant.MAX_MESSAGE_LENGTH:
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
        for separator in constant.TERMS:
            message, sep, remainder = self.buffer.partition(bytes([separator]))
            if sep:
                if len(message) > constant.MAX_MESSAGE_LENGTH:
                    message = message[:constant.MAX_MESSAGE_LENGTH]
                self.buffer = remainder
                return message


class GelfProtocol(SyslogProtocol):
    async def _process_data(self, data):
        if self.transport and self.transport.is_closing():
            return

        self.buffer.extend(data)
        while self.buffer:
            message = None

            if self.buffer[0] == constant.OPENBRACKET:
                message = self._get_non_transparent_framed_message()
            elif self.buffer[0:1] == constant.ZLIB_MAGIC:
                message = self._get_zlib_message()
            elif self.buffer[0:2] == constant.GZIP_MAGIC:
                message = self._get_gzip_message()
            elif self.buffer[0] in constant.TERMS:
                del self.buffer[0]
            elif self.buffer[0] in constant.METHODS:
                self._close_with_http()
            else:
                self._close_with_error('{0} unable to determine framing for message: {1}'.format(self.__class__.__name__, self.buffer))

            if message:
                await self.sink.write(bytes(message))
            else:
                return

    def _get_zlib_message(self):
        try:
            return zlib.decompress(self.buffer)
        except:
            logger.error('ZLIB decompression failed', exc_info=True)
        finally:
            self.buffer.clear()

    def _get_gzip_message(self):
        try:
            return gzip.decompress(self.buffer)
        except:
            logger.error('GZIP decompression failed', exc_info=True)
        finally:
            self.buffer.clear()


class SecureSyslogProtocol(SSLProtocol):
    PROTOCOL = SyslogProtocol

    def __init__(self, sslcontext, *args, **kwargs):
        loop = get_event_loop()
        super(SecureSyslogProtocol, self).__init__(
            loop=loop,
            app_protocol=self.PROTOCOL(*args, **kwargs),
            sslcontext=sslcontext,
            waiter=None,
            server_side=True
        )


class SecureGelfProtocol(SecureSyslogProtocol):
    PROTOCOL = GelfProtocol


class DatagramSyslogProtocol(SyslogProtocol):
    def connection_made(self, transport):
        # Don't store UDP transport to avoid closing it on parse failure
        pass

    def datagram_received(self, data, addr):
        # Append terminator to simplify parsing
        self.data_received(data + b'\x00')


class DatagramGelfProtocol(GelfProtocol, DatagramSyslogProtocol):
    __slots__ = ['chunks']

    def __init__(self, *args, **kwargs):
        super(DatagramGelfProtocol, self).__init__(*args, **kwargs)
        self.chunks = dict()

    def datagram_received(self, data, addr):
        if data[0:2] == constant.GELF_MAGIC:
            data = self._process_chunk(data)
            if not data:
                return
        super(DatagramGelfProtocol, self).datagram_received(data, addr)

    def _process_chunk(self, data):
        chunk = ChunkedMessage(data)
        # Abuse the fact that ChunkedMessage hashes by ID
        if chunk in self.chunks:
            message = self.chunks[chunk].update(chunk)
        else:
            self.chunks[chunk] = chunk
            message = chunk.update()

        if message:
            del self.chunks[chunk]
        return message


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
