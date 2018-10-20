import gzip
import logging
import time
import zlib
from asyncio import ensure_future, get_event_loop
from asyncio.sslproto import SSLProtocol

from . import constant
from .gelf import ChunkedMessage

logger = logging.getLogger(__name__)


class DefaultProtocol(object):
    def __init__(self, *args, **kwargs):
        raise NotImplementedError


class BaseLoggingProtocol(object):
    __slots__ = ['_sink', '_length', '_buffer', '_transport', '_sockname']
    PROTOCOL = DefaultProtocol

    def __init__(self, sink):
        self._sink = sink
        self._length = 0
        self._buffer = bytearray()
        self._transport = None
        self._sockname = None

    def connection_made(self, transport):
        self._sockname = transport.get_extra_info('sockname')
        if hasattr(transport, 'sendto'):
            return
        else:
            self._transport = transport

    def data_received(self, data, addr=None):
        ensure_future(self._process_data(data, addr))

    def datagram_received(self, data, addr):
        # Append terminator to simplify parsing
        self.data_received(data + b'\x00', addr)

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

    def _close_with_error(self, message=None):
        if message:
            logger.error(message)
        if self._buffer:
            self._buffer.clear()
        if self._transport:
            self._transport.close()

    def _close_with_http(self):
        if self._buffer:
            self._buffer.clear()
        if self._transport:
            logger.debug('Sending HTTP response and closing connection')
            self._transport.write(b'HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n')
            self._transport.close()

    def _get_non_transparent_framed_message(self):
        """
        Handle non-transparently-framed messages by searching for one of several possible terminators
        https://tools.ietf.org/html/rfc6587#section-3.4.2
        """
        for separator in constant.TERMS:
            message, sep, remainder = self._buffer.partition(bytes([separator]))
            if sep:
                if len(message) > constant.MAX_MESSAGE_LENGTH:
                    message = message[:constant.MAX_MESSAGE_LENGTH]
                self._buffer = remainder
                return message

    async def _write(self, addr, message):
        addr = addr or self._transport.get_extra_info('peername')
        await self._sink.write(addr[0], self._sockname[1], bytes(message), time.time())

    async def _process_data(self, data, addr=None):
        raise NotImplementedError


class BaseSecureLoggingProtocol(SSLProtocol):
    def __init__(self, sslcontext, *args, **kwargs):
        loop = get_event_loop()
        super(BaseSecureLoggingProtocol, self).__init__(
            loop=loop,
            app_protocol=self.PROTOCOL(*args, **kwargs),
            sslcontext=sslcontext,
            waiter=None,
            server_side=True
        )


class SyslogProtocol(BaseLoggingProtocol):
    async def _process_data(self, data, addr=None):
        if self._transport and self._transport.is_closing():
            return

        self._buffer.extend(data)
        while self._buffer:
            message = None

            if self._length or self._buffer[0] in constant.DIGITS:
                message = self._get_octet_counted_message()
            elif self._buffer[0] in constant.TERMS:
                del self._buffer[0]
            elif self._buffer[0] in constant.METHODS:
                self._close_with_http()
            else:
                message = self._get_non_transparent_framed_message()

            if message:
                await self._write(addr, message)
            else:
                return

    def _get_octet_counted_message(self):
        """
        Handle octet-counted messages by reading a space-separated length prefix,
        followed by the specified number of bytes. If insufficient bytes have been
        accumulated, remember the length and try again when we get more data.
        https://tools.ietf.org/html/rfc6587#section-3.4.1
        https://tools.ietf.org/html/rfc5425#section-4.3.1
        """
        if not self._length:
            length, sep, remainder = self._buffer.partition(b' ')
            if sep:
                try:
                    self._length = int(length)
                    if self._length < 0 or self._length > constant.MAX_MESSAGE_LENGTH:
                        raise ValueError
                    self._buffer = remainder
                except ValueError:
                    # Handle as random chunk of log noncompliant message that happens to start with numbers
                    return self._buffer
            else:
                return

        if len(self._buffer) >= self._length:
            message = self._buffer[:self._length]
            del self._buffer[:self._length]
            self._length = 0
            return message
        else:
            return


class GelfProtocol(BaseLoggingProtocol):
    async def _process_data(self, data, addr=None):
        if self._transport and self._transport.is_closing():
            return

        self._buffer.extend(data)
        while self._buffer:
            message = None

            if self._buffer[0] == constant.OPENBRACKET:
                message = self._get_non_transparent_framed_message()
            elif self._buffer[0:1] == constant.ZLIB_MAGIC:
                message = self._get_zlib_message()
            elif self._buffer[0:2] == constant.GZIP_MAGIC:
                message = self._get_gzip_message()
            elif self._buffer[0] in constant.TERMS:
                del self._buffer[0]
            elif self._buffer[0] in constant.METHODS:
                self._close_with_http()
            else:
                self._close_with_error('{0} unable to determine framing for message: {1}'.format(self.__class__.__name__, self._buffer))

            if message:
                await self._write(addr, message)
            else:
                return

    def _get_zlib_message(self):
        try:
            return zlib.decompress(self._buffer)
        except Exception:
            logger.error('ZLIB decompression failed', exc_info=True)
        finally:
            self._buffer.clear()

    def _get_gzip_message(self):
        try:
            return gzip.decompress(self._buffer)
        except Exception:
            logger.error('GZIP decompression failed', exc_info=True)
        finally:
            self._buffer.clear()


class SecureSyslogProtocol(BaseSecureLoggingProtocol):
    PROTOCOL = SyslogProtocol


class SecureGelfProtocol(BaseSecureLoggingProtocol):
    PROTOCOL = GelfProtocol


class DatagramSyslogProtocol(SyslogProtocol):
    pass


class DatagramGelfProtocol(GelfProtocol):
    __slots__ = ['_chunks']

    def __init__(self, *args, **kwargs):
        super(DatagramGelfProtocol, self).__init__(*args, **kwargs)
        self._chunks = dict()

    def datagram_received(self, data, addr):
        if data[0:2] == constant.GELF_MAGIC:
            data = self._process_chunk(data)
            if not data:
                return
        super(DatagramGelfProtocol, self).data_received(data, addr)

    # TODO - enforce 5 second chunk reassembly window
    def _process_chunk(self, data):
        chunk = ChunkedMessage(data)
        # Abuse the fact that ChunkedMessage hashes by ID
        if chunk in self._chunks:
            message = self._chunks[chunk].update(chunk)
        else:
            self._chunks[chunk] = chunk
            message = chunk.update()

        if message:
            del self._chunks[chunk]
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
