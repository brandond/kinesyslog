import gzip
import logging
import time
import zlib
from asyncio import CancelledError, Event
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
        self._buffer_ready = Event()
        self._transport = None
        self._sockname = [b'0.0.0.0', 0]
        self._peername = [b'0.0.0.0', 0]
        self._pause_reading = False
        self._reader_task = None

    def connection_made(self, transport):
        self._sockname = transport.get_extra_info('sockname')
        self._reader_task = self._loop.create_task(self._buffer_reader())
        self._reader_task.add_done_callback(self._buffer_reader_done)

        if not hasattr(transport, 'sendto'):
            self._transport = transport
            self._peername = transport.get_extra_info('peername')

    def data_received(self, data):
        self._inc_counter(constant.STAT_MESSAGE_BYTES, len(data))
        self._buffer.extend(data)
        self._buffer_ready.set()

    def datagram_received(self, data, addr):
        if self._buffer:
            logger.warn('Clearing unprocessed datagram buffer of length {0}'.format(len(self._buffer)))
            self._buffer.clear()

        self._peername = addr
        self.data_received(data + b'\x00')

    def eof_received(self):
        pass

    def error_received(self, exc):
        self._close_with_error('Protocol error on {0}'.format(self._sockname[1]), exc)

    def connection_lost(self, exc):
        self._reader_task.cancel()

    def pause_writing(self):
        pass

    def resume_writing(self):
        pass

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
        for separator in constant.TERMS:
            message, sep, remainder = self._buffer.partition(bytes([separator]))
            if sep:
                if len(message) > constant.MAX_MESSAGE_LENGTH:
                    message = message[:constant.MAX_MESSAGE_LENGTH]
                self._buffer = remainder
                return message
        if len(self._buffer) > constant.MAX_MESSAGE_LENGTH:
            self._close_with_error('Maximum message length exceeded without finding terminating trailer character')

    async def _write(self, message):
        self._inc_counter(constant.STAT_MESSAGE_COUNT, 1)
        await self._sink.write(self._peername[0], self._sockname[1], bytes(message), time.time())

    async def _buffer_reader(self):
        while True:
            await self._buffer_ready.wait()

            if len(self._buffer) >= constant.MAX_MESSAGE_BUFFER and self._transport:
                logger.debug("Pausing")
                self._pause_reading = True
                self._transport.pause_reading()

            await self._process_data()

            if self._pause_reading and self._transport:
                if len(self._buffer) < constant.MAX_MESSAGE_BUFFER:
                    logger.debug("Unpausing")
                    self._pause_reading = False
                    self._transport.resume_reading()
                else:
                    self._close_with_error('Maximum buffer length exceeded without finding valid message')

            self._buffer_ready.clear()

    def _buffer_reader_done(self, task):
        try:
            task.result()
        except CancelledError:
            pass
        except Exception as e:
            self._close_with_error('Error processing buffer on {0}'.format(self._sockname[1]), e)

    def _inc_counter(self, counter, value):
        labels = {'source': self._peername[0], 'port': self._sockname[1]}
        self._registry.get(counter).add(labels=labels, value=value)

    async def _process_data(self):
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
    async def _process_data(self):
        while self._buffer:
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
                await self._write(message)
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
            length, sep, remainder = self._buffer.partition(b' ')
            if sep:
                try:
                    self._length = int(length)
                    self._buffer = remainder
                except ValueError:
                    # Handle as random chunk of noncompliant message that happens to start with a digit
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
    async def _process_data(self):
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
            else:
                self._close_with_error('{0} unable to determine framing for message: {1}'.format(self.__class__.__name__, self._buffer))

            if message:
                await self._write(message)
            else:
                break

    # FIXME: this is all incredibly broken if there's actually more than one packet in the buffer
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
        super(DatagramGelfProtocol, self).data_received(data)

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
