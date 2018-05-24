import logging
import math
import time
from asyncio import get_event_loop
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from gzip import compress

from boto3 import Session

import ujson

from . import constant

logger = logging.getLogger(__name__)


class MessageSink(object):
    __slots__ = ['spool', 'loop', 'executor', 'size', 'count', 'messages', 'flushed', 'message_class', 'account', 'group_prefix']

    def __init__(self, spool, message_class, group_prefix):
        self.spool = spool
        self.message_class = message_class
        self.group_prefix = group_prefix
        self.loop = get_event_loop()
        self.executor = ProcessPoolExecutor(max_workers=1)
        self.executor._start_queue_management_thread()
        self._schedule_flush()
        self.clear()
        self.account = '000000000000'
        try:
            session = Session(profile_name=spool.profile_name)
            client = session.client('sts', config=spool.config)
            self.account = client.get_caller_identity()['Account']
        except Exception:
            logger.warn('Unable to determine AWS Account ID; using default value.')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.flush()

    async def write(self, source, dest, message, timestamp):
        self.messages[(source, dest)].append((message, timestamp))
        self.size += len(message)
        self.count += 1
        if self.size > constant.FLUSH_SIZE:
            await self.flush_async()
        return len(message)

    def clear(self):
        self.size = 0
        self.count = 0
        self.messages = defaultdict(list)
        self.flushed = time.time()

    async def flush_async(self):
        self.loop.run_in_executor(self.executor, self._spool_messages,
                                  self.spool, self.messages, self.size, self.message_class, self.account, self.group_prefix)
        self.clear()

    def flush(self):
        self._spool_messages(self.spool, self.messages, self.size, self.message_class, self.account, self.group_prefix)
        self.clear()

    def _schedule_flush(self):
        self.loop.call_later(constant.TIMER_INTERVAL, self._flush_timer)

    def _flush_timer(self):
        logger.debug('flush timer: messages={0} size={1} age={2}'.format(self.count, self.size, time.time() - self.flushed))
        if self.messages and time.time() - self.flushed >= constant.FLUSH_TIME:
            self.loop.create_task(self.flush_async())
        self._schedule_flush()

    @classmethod
    def _spool_messages(cls, spool, messages, size, message_class, account, group_prefix):
        for (source, dest), values in messages.items():
            group = '{0}/{1}/{2}'.format(group_prefix, message_class.name, dest)
            events = message_class.create_events(source, values)
            record = cls._prepare_record(account, group, source, events)
            compressed_record = cls._compress_record(record)
            logger.debug('Events for {0} > {1} compressed from {2} to {3} bytes (with JSON framing)'.format(group, source, size, len(compressed_record)))

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
                    record_part = cls._prepare_record(account, group, source, record['logEvents'][start:start+size])
                    compressed_record = cls._compress_record(record_part)
                    spool.write(compressed_record)
                    start += size
            else:
                spool.write(compressed_record)

    @classmethod
    def _prepare_record(cls, owner, group, stream, events, filters=[], type='DATA_MESSAGE'):
        if not isinstance(filters, list):
            filters = [filters]

        if not filters:
            filters = [group]

        return {
            'owner': owner,
            'logGroup': group,
            'logStream': stream,
            'subscriptionFilters': filters,
            'messageType': type,
            'logEvents': events,
        }

    @classmethod
    def _compress_record(cls, record):
        return compress(ujson.dumps(record, escape_forward_slashes=False).encode())
