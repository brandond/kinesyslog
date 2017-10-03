import json
import logging
import math
import time
from asyncio import get_event_loop
from concurrent.futures import ProcessPoolExecutor
from gzip import GzipFile
from io import BytesIO, TextIOWrapper

from .message import create_events

logger = logging.getLogger(__name__)

FLUSH_TIME = 60
FLUSH_SIZE = 1024 * 1024 * 4
MAX_RECORD_SIZE = 1024 * 1000
TIMER_INTERVAL = 10


class MessageSink(object):
    __slots__ = ['spool', 'loop', 'executor', 'size', 'messages', 'flushed']

    def __init__(self, spool):
        self.spool = spool
        self.loop = get_event_loop()
        self.executor = ProcessPoolExecutor()
        self._schedule_flush()
        self.clear()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.flush()

    async def write(self, b):
        if not b:
            return
        self.messages += [b]
        self.size += len(b)
        if self.size > FLUSH_SIZE:
            await self.flush_async()
        return len(b)

    def clear(self):
        self.size = 0
        self.messages = list()
        self.flushed = time.time()

    async def flush_async(self):
        self.loop.run_in_executor(self.executor, self._spool_messages, self.spool, self.messages)
        self.clear()

    def flush(self):
        self._spool_messages(self.spool, self.messages)
        self.clear()

    def _schedule_flush(self):
        self.loop.call_later(TIMER_INTERVAL, self._flush_timer)

    def _flush_timer(self):
        logger.debug('flush timer: messages={0} size={1} age={2}'.format(len(self.messages), self.size, time.time() - self.flushed))
        if self.messages and time.time() - self.flushed >= FLUSH_TIME:
            self.loop.create_task(self.flush_async())
        self._schedule_flush()

    @classmethod
    def _spool_messages(cls, spool, messages):
        if not messages:
            return
        record = cls._prepare_record(create_events(messages))
        compressed_record = cls._compress_record(record)

        if len(compressed_record) > MAX_RECORD_SIZE:
            # This approach naievely hopes that splitting a record into even parts will put it
            # below the max record size. Further tuning may be required.
            split_count = math.ceil(len(compressed_record) / MAX_RECORD_SIZE)
            logger.warning('Compressed record size of {0} bytes exceeds maximum Firehose record size of {1} bytes; splitting into {2} records'.format(
                len(compressed_record),
                MAX_RECORD_SIZE,
                split_count
            ))
            start = 0
            size = int(len(record['logEvents']) / split_count)
            while start < len(record['logEvents']):
                record_part = cls._prepare_record(record['logEvents'][start:start+size])
                compressed_record = cls._compress_record(record_part)
                spool.write(compressed_record)
                start += size
        else:
            spool.write(compressed_record)

    @classmethod
    def _prepare_record(cls, events):
        return {
            'owner': '000000000000',
            'logGroup': 'syslog',
            'logStream': 'syslog',
            'subscriptionFilters': ['syslog'],
            'messageType': 'DATA_MESSAGE',
            'logEvents': events,
        }

    @classmethod
    def _compress_record(cls, record):
        with BytesIO() as bytebuf:
            with GzipFile(fileobj=bytebuf, mode='wb') as gzipbuf:
                with TextIOWrapper(buffer=gzipbuf, encoding='utf-8') as textbuf:
                    json.dump(record, textbuf)
                    logger.debug('Record compressed from {0} to {1} bytes'.format(textbuf.tell(), bytebuf.tell()))
            return bytebuf.getvalue()
