import logging
import math
import signal
import socket
import asyncio
from collections import defaultdict
from gzip import compress
from multiprocessing import Process

import msgpack
from msgpack.exceptions import UnpackValueError

import ujson

from . import constant, util

logger = logging.getLogger(__name__)
RSOCKS = list()
WSOCKS = list()


class MessageSink(object):
    def __init__(self, spool, server, message_class, group_prefix):
        (rsock, wsock) = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        rsock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, constant.MAX_MESSAGE_BUFFER)
        wsock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, constant.MAX_MESSAGE_BUFFER)
        rsock.setblocking(False)
        wsock.setblocking(False)
        RSOCKS.append(rsock)
        WSOCKS.append(wsock)

        self.packer = msgpack.Packer()
        self.lock = asyncio.Lock()
        self.stats = defaultdict(lambda: defaultdict(lambda: dict(messages=0, bytes=0)))
        self.loop = asyncio.get_event_loop()
        self.sock = wsock
        self.worker = MessageSinkWorker(spool, server, message_class, group_prefix, rsock, daemon=True)

    async def write(self, source, dest, message, timestamp):
        length = len(message)
        if length > constant.MAX_MESSAGE_LENGTH:
            logger.warn('Truncating {} byte message'.format(length))
            message = message[:constant.MAX_MESSAGE_LENGTH]
            length = constant.MAX_MESSAGE_LENGTH

        async with self.lock:
            await self.loop.sock_sendall(self.sock, self.packer.pack([source, dest, message, timestamp]))

        self.stats[dest][source]['messages'] += 1
        self.stats[dest][source]['bytes'] += length

    def __enter__(self):
        self.worker.start()
        util.close_all_socks(RSOCKS)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        logger.debug('Worker shutdown requested')
        while self.worker.is_alive():
            self.worker.terminate()
            self.worker.join(1)
        util.close_all_socks(WSOCKS)
        logger.debug('Worker shutdown complete')


class MessageSinkWorker(Process):
    def __init__(self, spool, server, message_class, group_prefix, sock, *args, **kwargs):
        super(MessageSinkWorker, self).__init__(*args, **kwargs)
        self.spool = spool
        self.server = server
        self.message_class = message_class
        self.group_prefix = group_prefix
        self.sock = sock
        self.events = defaultdict(list)
        self.account = '000000000000'

        try:
            client = spool.session.client('sts', config=spool.config)
            self.account = client.get_caller_identity()['Account']
        except Exception:
            logger.warn('Unable to determine AWS Account ID; using default value.', exc_info=True)

    def run(self):
        util.setproctitle('{0} ({1}:{2})'.format(__name__,  self.server.PROTOCOL.__name__, self.server._port))
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        util.close_all_socks(WSOCKS)
        self.loop = util.new_event_loop()
        self.loop.add_signal_handler(signal.SIGTERM, self.stop)
        self.clear()
        self.schedule_flush()
        self.loop.add_reader(self.sock.fileno(), self.read)
        logger.debug('Worker starting')
        self.loop.run_forever()

        # Time passes...

        logger.debug('Worker shutting down')
        self.loop.remove_reader(self.sock.fileno())
        util.close_all_socks(RSOCKS)
        tasks = asyncio.gather(*asyncio.Task.all_tasks(loop=self.loop), loop=self.loop, return_exceptions=True)
        tasks.add_done_callback(lambda f: self.loop.stop())
        tasks.cancel()
        while not tasks.done() and not self.loop.is_closed():
            self.loop.run_forever()

        raise SystemExit(0)

    def stop(self):
        self.loop.stop()
        self.flush()

    def read(self):
        try:
            buff = self.sock.recv(constant.MAX_MESSAGE_BUFFER)
            if buff:
                args = msgpack.unpackb(buff)
                self.loop.call_soon(self.add_message, *args)
        except BlockingIOError:
            pass
        except UnpackValueError:
            logger.warn('Failed to unpack message', exc_info=True)

    def add_message(self, source, dest, message, timestamp):
        source = source.decode('utf-8', 'backslashreplace')
        event = self.message_class.create_event(source, message, timestamp)
        self.events[(source, dest)].append(event)
        self.size += len(message)
        self.count += 1
        if self.size > constant.FLUSH_SIZE:
            self.flush()

    def schedule_flush(self):
        self.loop.call_later(constant.TIMER_INTERVAL, self.flush_check)

    def flush_check(self):
        age = self.loop.time() - self.flushed
        logger.debug('flush check: messages={0} size={1} age={2}'.format(self.count, self.size, age))
        if self.events and age >= constant.FLUSH_TIME:
            self.loop.call_soon(self.flush)
        self.schedule_flush()

    def flush(self):
        for (source, dest), events in self.events.items():
            group = '{0}/{1}/{2}'.format(self.group_prefix, self.message_class.name, dest)
            record = self._prepare_record(self.account, group, source, events)
            compressed_record = self._compress_record(record)
            logger.debug('Events for {0} > {1} compressed from {2} to {3} bytes (with JSON framing)'.format(group, source, self.size, len(compressed_record)))

            if len(compressed_record) > constant.MAX_RECORD_SIZE:
                # This approach naievely hopes that splitting a record into even parts will put it
                # below the max record size. Further tuning may be required.
                split_count = math.ceil(len(compressed_record) / constant.MAX_RECORD_SIZE)
                logger.warn('Compressed record size of {0} bytes exceeds maximum Firehose record size of {1} bytes; splitting into {2} records'.format(
                    len(compressed_record),
                    constant.MAX_RECORD_SIZE,
                    split_count
                ))
                start = 0
                size = int(len(record['logEvents']) / split_count)
                while start < len(record['logEvents']):
                    record_part = self._prepare_record(self.account, group, source, record['logEvents'][start:start+size])
                    compressed_record = self._compress_record(record_part)
                    logger.debug('Events[{0}:{1}] compressed to {2} bytes (with JSON framing)'.format(start, start+size, len(compressed_record)))
                    self.spool.write(compressed_record)
                    start += size
            else:
                self.spool.write(compressed_record)
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
    def _compress_record(cls, record):
        return compress(ujson.dumps(record, escape_forward_slashes=False).encode())
