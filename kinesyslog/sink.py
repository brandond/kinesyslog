import asyncio
import ctypes
import logging
import math
import random
import signal
from collections import defaultdict
from encodings.utf_8 import StreamWriter
from gzip import GzipFile
from io import BytesIO
from multiprocessing import Process

import ujson

from . import constant, ringbuffer, util

logger = logging.getLogger(__name__)
src_buf = ctypes.create_string_buffer(constant.MAX_SOURCE_LENGTH)
msg_buf = ctypes.create_string_buffer(constant.MAX_MESSAGE_LENGTH)


def _pack_c_messages(source, dest, messages, timestamp):
    count = len(messages)
    c_messages = MessageBatch(timestamp=timestamp, dest=dest, count=count)
    src_buf.raw = source.encode()
    ctypes.memmove(c_messages.source, src_buf, constant.MAX_SOURCE_LENGTH)
    for i in range(0, count):
        length = len(messages[i])
        c_messages.messages[i].length = length
        msg_buf.raw = messages[i][:length]
        ctypes.memmove(c_messages.messages[i].message, msg_buf, length)
    return c_messages


def _unpack_c_messages(c_messages):
    source = ctypes.string_at(c_messages.source)
    dest = c_messages.dest
    timestamp = c_messages.timestamp
    messages = [None] * c_messages.count
    for i in range(0, c_messages.count):
        messages[i] = ctypes.string_at(c_messages.messages[i].message, c_messages.messages[i].length)
    return (source, dest, messages, timestamp)


class Message(ctypes.Structure):
    _fields_ = [
        ('length', ctypes.c_uint),
        ('message', ctypes.c_ubyte * constant.MAX_MESSAGE_LENGTH),
    ]


class MessageBatch(ctypes.Structure):
    _fields_ = [
        ('timestamp', ctypes.c_double),
        ('dest', ctypes.c_uint),
        ('count', ctypes.c_uint),
        ('source', ctypes.c_ubyte * constant.MAX_SOURCE_LENGTH),
        ('messages', Message * constant.MAX_MESSAGE_COUNT),
    ]


class MessageSink(object):
    def __init__(self, spool, server, message_class, group_prefix, account):
        self.loop = asyncio.get_event_loop()
        self.ring = ringbuffer.RingBuffer(c_type=MessageBatch, slot_count=constant.MAX_MESSAGE_COUNT)
        self.worker = MessageSinkWorker(spool, server, message_class, group_prefix, account, self.ring, self.ring.new_reader(), daemon=True)

    def __enter__(self):
        self.ring.new_writer()
        self.worker.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.ring:
            self.ring.writer_done()
            self.ring = None
        if self.worker.is_alive():
            logger.debug('Waiting for worker process shutdown')
            self.worker.join()
            logger.debug('Worker process shutdown complete')

    async def write(self, source, dest, messages, timestamp):
        if self.ring:
            await self.write_messages(source, dest, messages, timestamp)

    async def write_messages(self, source, dest, messages, timestamp):
        c_messages = _pack_c_messages(source, dest, messages, timestamp)
        while True:
            try:
                return self.ring.try_write(c_messages)
            except ringbuffer.WaitingForReaderError:
                await asyncio.sleep(0)


class MessageSinkWorker(Process):
    def __init__(self, spool, server, message_class, group_prefix, account, ring, reader, *args, **kwargs):
        super(MessageSinkWorker, self).__init__(*args, **kwargs)
        self.spool = spool
        self.server = server
        self.message_class = message_class
        self.group_prefix = group_prefix
        self.events = defaultdict(list)
        self.account = account
        self.ring = ring
        self.reader = reader

    def run(self):
        logger.debug('Worker starting')
        util.setproctitle('{0} ({1}:{2})'.format(__name__,  self.server.PROTOCOL.__name__, self.server._port))
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        random.seed()

        self.loop = util.new_event_loop()
        self.clear()
        self.loop.create_task(self.flush_check())
        self.loop.run_until_complete(self.read())

        # Time passes...

        logger.debug('Worker shutting down')
        self.loop.run_until_complete(self.loop.shutdown_asyncgens())
        self.flush()
        raise SystemExit(0)

    async def read(self):
        async for c_messages in self.read_messages():
            self.loop.call_soon(self.add_messages, *_unpack_c_messages(c_messages))

    async def read_messages(self):
        while True:
            try:
                for c_messages in self.ring.blocking_read(self.reader, length=0, timeout=1.0):
                    yield c_messages
            except ringbuffer.WaitingForWriterError:
                pass
            except ringbuffer.WriterFinishedError:
                logger.debug('Writer finished, shutting down')
                break
            await asyncio.sleep(0)

    def add_messages(self, source, dest, messages, timestamp):
        source = source.decode()
        for message in messages:
            event = self.message_class.create_event(source, message, timestamp)
            self.events[(source, dest)].append(event)
            self.size += len(message)
            self.count += 1
        if self.size > constant.FLUSH_SIZE:
            self.flush()

    async def flush_check(self):
        while True:
            await asyncio.sleep(constant.TIMER_INTERVAL)
            age = self.loop.time() - self.flushed
            logger.debug('flush check: messages={0} size={1} age={2}'.format(self.count, self.size, age))
            if self.events and age >= constant.FLUSH_TIME:
                self.loop.call_soon(self.flush)

    def flush(self):
        compress_buf = BytesIO()
        for (source, dest), events in self.events.items():
            group = '{0}/{1}/{2}'.format(self.group_prefix, self.message_class.name, dest)
            record = self._prepare_record(self.account, group, source, events)
            self._compress_record(record, compress_buf)
            logger.debug('Events for {0} > {1} compressed from {2} to {3} bytes (with JSON framing)'.format(group, source, self.size, compress_buf.tell()))

            if compress_buf.tell() > constant.MAX_RECORD_SIZE:
                # This approach naievely hopes that splitting a record into even parts will put it
                # below the max record size. Further tuning may be required.
                split_count = math.ceil(compress_buf.tell() / constant.MAX_RECORD_SIZE)
                logger.warn('Compressed record size of {0} bytes exceeds maximum Firehose record size of {1} bytes; splitting into {2} records'.format(
                    compress_buf.tell(),
                    constant.MAX_RECORD_SIZE,
                    split_count
                ))
                start = 0
                size = int(len(record['logEvents']) / split_count)
                while start < len(record['logEvents']):
                    record_part = self._prepare_record(self.account, group, source, record['logEvents'][start:start+size])
                    self._compress_record(record_part, compress_buf)
                    logger.debug('Events[{0}:{1}] compressed to {2} bytes (with JSON framing)'.format(start, start+size, compress_buf.tell()))
                    self.spool.write(compress_buf)
                    start += size
            else:
                self.spool.write(compress_buf)
        self.clear()

    def clear(self):
        self.size = 0
        self.count = 0
        self.events.clear()
        self.flushed = self.loop.time()

    @classmethod
    def _prepare_record(cls, account, group, stream, events, filters=[], type='DATA_MESSAGE'):
        if not isinstance(filters, list):
            filters = [filters]

        if not filters:
            filters = [group]

        return {
            'owner': account,
            'logGroup': group,
            'logStream': stream,
            'subscriptionFilters': filters,
            'messageType': type,
            'logEvents': events,
            }

    @classmethod
    def _compress_record(cls, record, buf):
        buf.seek(0, 0)
        buf.truncate()
        with GzipFile(fileobj=buf, mode='wb', compresslevel=9) as g:
            with StreamWriter(stream=g, errors='backslashescape') as s:
                ujson.dump(record, s, escape_forward_slashes=False)
