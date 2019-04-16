import gzip
import logging
import time
import zlib
from asyncio import CancelledError
from asyncio.sslproto import SSLProtocol

from . import constant
from .gelf import ChunkedMessage

logger = logging.getLogger(__name__)


class DefaultProtocol(object):
    def __init__(self, *args, **kwargs):
        raise NotImplementedError()


class BaseLoggingProtocol(object):
    PROTOCOL = DefaultProtocol

    def __init__(self, sink, loop, registry):
        self._sink = sink
        self._loop = loop
        self._registry = registry
        self._length = 0
        self._discard = 0
        self._buffer = bytearray()
        self._transport = None
        self._sockname = [b'0.0.0.0', 0]
        self._paused = False

    def connection_made(self, transport):
        self._sockname = transport.get_extra_info('sockname')
        if hasattr(transport, 'sendto'):
            return
        else:
            self._transport = transport

    def data_received(self, data, addr=None):
        if self._loop.is_running():
            addr = addr or self._transport.get_extra_info('peername')
            task = self._loop.create_task(self._feed_data(data, addr))
            task.add_done_callback(self._feed_done)

    def datagram_received(self, data, addr):
        # Append terminator to simplify parsing
        self.data_received(data + b'\x0A', addr)

    def eof_received(self):
        pass

    def error_received(self, exc):
        self._close_with_error('Protocol error on {0}'.format(self._sockname[1]), exc)

    def connection_lost(self, exc):
        if self._transport:
            self.data_received(b'')

    def pause_writing(self):
        pass

    def resume_writing(self):
        pass

    def _inc_counter(self, addr, counter, value):
        if self._registry.active:
            counter = self._registry.collectors[counter]
            labels = '{{"port": {0}, "source": "{1}"}}'.format(self._sockname[1], addr[0])
            try:
                cur_value = counter.values.store[labels]
            except KeyError:
                cur_value = 0
            counter.values.store[labels] = cur_value + value

    def _feed_done(self, task):
        try:
            task.result()
        except CancelledError:
            pass
        except Exception as e:
            self._close_with_error('Error processing buffer on {0}'.format(self._sockname[1]), e)

    def _close_with_error(self, message=None, exception=None):
        if message:
            logger.error('Closing connection: {}'.format(message), exc_info=exception)
        if self._transport:
            self._transport.close()
        if self._buffer:
            self._buffer.clear()

    def _get_non_transparent_framed_message(self):
        """
        Handle non-transparently-framed messages by searching for one of several possible terminators
        https://tools.ietf.org/html/rfc6587#section-3.4.2
        """
        for sep in constant.TERMS:
            pos = self._buffer.find(sep)
            if pos != -1:
                message = self._buffer[:pos]
                del self._buffer[:pos+1]
                return message
        if len(self._buffer) > constant.MAX_MESSAGE_BUFFER:
            self._close_with_error('Maximum buffer size exceeded without finding terminating trailer character')

    async def _write(self, addr, messages, timestamp):
        addr = addr or self._transport.get_extra_info('peername')
        self._inc_counter(addr, constant.STAT_MESSAGE_COUNT, len(messages))
        await self._sink.write(addr[0], self._sockname[1], messages, timestamp)

    async def _feed_data(self, data, addr):
        self._buffer.extend(data)
        self._inc_counter(addr, constant.STAT_MESSAGE_BYTES, len(data))

        if len(self._buffer) >= constant.MAX_MESSAGE_BUFFER and self._transport:
            self._paused = True
            self._transport.pause_reading()

        await self._process_data(addr)

        if self._paused and self._transport:
            if len(self._buffer) < constant.MAX_MESSAGE_BUFFER:
                self._paused = False
                self._transport.resume_reading()
            else:
                self._close_with_error('Maximum buffer length exceeded without yeilding valid messages')

    async def _process_data(self, addr=None):
        raise NotImplementedError


class BaseSecureLoggingProtocol(SSLProtocol):
    def __init__(self, sslcontext, loop, *args, **kwargs):
        super(BaseSecureLoggingProtocol, self).__init__(
            loop=loop,
            app_protocol=self.PROTOCOL(loop=loop, *args, **kwargs),
            sslcontext=sslcontext,
            waiter=None,
            server_side=True
        )


class SyslogProtocol(BaseLoggingProtocol):
    async def _process_data(self, addr):
        timestamp = time.time()
        messages = []

        while True:
            while self._buffer and len(messages) < constant.MAX_MESSAGE_COUNT:
                message = None

                if self._length:
                    message = self._get_octet_counted_message()
                elif self._discard:
                    self._discard_excess_bytes()
                elif self._buffer[0] in constant.DIGITS:
                    message = self._get_octet_counted_message()
                elif self._buffer[0] in constant.TERMS:
                    del self._buffer[0]
                else:
                    message = self._get_non_transparent_framed_message()

                if message:
                    messages.append(message[:constant.MAX_MESSAGE_LENGTH])
                else:
                    break

            if messages:
                await self._write(addr, messages, timestamp)
                messages.clear()
            else:
                break

    def _discard_excess_bytes(self):
        """
        Discard excess bytes from overlength message
        """
        discard_len = min(self._discard, len(self._buffer))
        del self._buffer[:discard_len]
        self._discard -= discard_len

    def _get_octet_counted_message(self):
        """
        Handle octet-counted messages by reading a space-separated length prefix,
        followed by the specified number of bytes. If insufficient bytes have been
        accumulated, remember the length and try again when we get more data.
        https://tools.ietf.org/html/rfc6587#section-3.4.1
        https://tools.ietf.org/html/rfc5425#section-4.3.1
        """
        if not self._length:
            pos = self._buffer.find(b' ')
            if pos != -1:
                try:
                    self._length = int(self._buffer[0:pos])
                    del self._buffer[0:pos+1]
                except ValueError:
                    # Handle as random chunk of log noncompliant message that happens to start with a digit
                    return self._get_non_transparent_framed_message()
            else:
                return

        if self._length > constant.MAX_MESSAGE_LENGTH:
            self._discard = self._length - constant.MAX_MESSAGE_LENGTH
            self._length = constant.MAX_MESSAGE_LENGTH

        if len(self._buffer) >= self._length:
            message = self._buffer[:self._length]
            del self._buffer[:self._length]
            self._length = 0
            return message
        else:
            return


class GelfProtocol(BaseLoggingProtocol):
    async def _process_data(self, addr):
        timestamp = time.time()
        messages = []

        while True:
            while self._buffer and len(messages) < constant.MAX_MESSAGE_COUNT:
                message = None

                if self._buffer[0] == constant.OPENBRACKET:
                    message = self._get_non_transparent_framed_message()
                elif self._buffer[0:1] == constant.ZLIB_MAGIC:
                    message = self._get_zlib_message()
                elif self._buffer[0:2] == constant.GZIP_MAGIC:
                    message = self._get_gzip_message()
                elif self._buffer[0] in constant.TERMS:
                    del self._buffer[0]
                else:
                    self._close_with_error('{0} unable to determine framing for message: {1}'.format(self.__class__.__name__, self._buffer))

                if message:
                    messages.append(message[:constant.MAX_MESSAGE_LENGTH])
                else:
                    break

            if messages:
                await self._write(addr, messages, timestamp)
                messages.clear()
            else:
                break

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
