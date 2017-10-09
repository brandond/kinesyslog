import logging
import math
import time
from asyncio import get_event_loop
from concurrent.futures import ProcessPoolExecutor
from gzip import compress

from boto3 import Session

import ujson as json

from . import constant
from .message import create_events

logger = logging.getLogger(__name__)


class MessageSink(object):
    __slots__ = ['spool', 'loop', 'executor', 'size', 'messages', 'flushed', 'message_type', 'account']

    def __init__(self, spool, message_type='syslog'):
        self.spool = spool
        self.message_type = message_type
        self.loop = get_event_loop()
        self.executor = ProcessPoolExecutor()
        self.executor._start_queue_management_thread()
        self._schedule_flush()
        self.clear()
        self.account = '000000000000'
        try:
            session = Session(profile_name=spool.profile_name)
            client = session.client('sts', config=spool.config)
            self.account = client.get_caller_identity()['Account']
        except:
            logger.warn('Unable to determine AWS Account ID; using default value.')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.flush()

    async def write(self, b):
        if not b:
            return
        self.messages += [b]
        self.size += len(b)
        if self.size > constant.FLUSH_SIZE:
            await self.flush_async()
        return len(b)

    def clear(self):
        self.size = 0
        self.messages = list()
        self.flushed = time.time()

    async def flush_async(self):
        self.loop.run_in_executor(self.executor, self._spool_messages, self.spool, self.messages, self.size, self.message_type, self.account)
        self.clear()

    def flush(self):
        self._spool_messages(self.spool, self.messages, self.size, self.message_type, self.account)
        self.clear()

    def _schedule_flush(self):
        self.loop.call_later(constant.TIMER_INTERVAL, self._flush_timer)

    def _flush_timer(self):
        logger.debug('flush timer: messages={0} size={1} age={2}'.format(len(self.messages), self.size, time.time() - self.flushed))
        if self.messages and time.time() - self.flushed >= constant.FLUSH_TIME:
            self.loop.create_task(self.flush_async())
        self._schedule_flush()

    @classmethod
    def _spool_messages(cls, spool, messages, size, message_type, account):
        if not messages:
            return
        record = cls._prepare_record(create_events(messages), message_type, account)
        compressed_record = cls._compress_record(record)
        logger.debug('Events compressed from {0} to {1} bytes (with JSON framing)'.format(size, len(compressed_record)))

        if len(compressed_record) > constant.MAX_RECORD_SIZE:
            # This approach naievely hopes that splitting a record into even parts will put it
            # below the max record size. Further tuning may be required.
            split_count = math.ceil(len(compressed_record) / constant.MAX_RECORD_SIZE)
            logger.warning('Compressed record size of {0} bytes exceeds maximum Firehose record size of {1} bytes; splitting into {2} records'.format(
                len(compressed_record),
                constant.MAX_RECORD_SIZE,
                split_count
            ))
            start = 0
            size = int(len(record['logEvents']) / split_count)
            while start < len(record['logEvents']):
                record_part = cls._prepare_record(record['logEvents'][start:start+size], message_type, account)
                compressed_record = cls._compress_record(record_part)
                spool.write(compressed_record)
                start += size
        else:
            spool.write(compressed_record)

    @classmethod
    def _prepare_record(cls, events, message_type, account):
        return {
            'owner': account,
            'logGroup': message_type,
            'logStream': message_type,
            'subscriptionFilters': [message_type],
            'messageType': 'DATA_MESSAGE',
            'logEvents': events,
        }

    @classmethod
    def _compress_record(cls, record):
        return compress(json.dumps(record).encode())
