import asyncio
import logging
import os
import signal
import socket
import threading
from asyncio import CancelledError
from glob import glob
from multiprocessing import Process
from tempfile import NamedTemporaryFile

import msgpack
from boto3 import Session
from botocore.config import Config
from msgpack.exceptions import UnpackValueError

from . import constant, util

logger = logging.getLogger(__name__)
RSOCKS = list()
WSOCKS = list()


class EventSpool(object):
    def __init__(self, delivery_stream, spool_dir, registry, region_name=None, profile_name=None):
        self.session = Session(profile_name=profile_name)
        self.config = Config(retries={'max_attempts': 10}, region_name=util.get_region(region_name, profile_name))
        self._validate_stream(self.session, self.config, delivery_stream)

        self.spool_dir = spool_dir
        self.registry = registry

        (rsock, wsock) = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
        rsock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, constant.MAX_MESSAGE_BUFFER)
        wsock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, constant.MAX_MESSAGE_BUFFER)
        rsock.setblocking(False)
        wsock.setblocking(False)
        RSOCKS.append(rsock)
        WSOCKS.append(wsock)

        self.lock = asyncio.Lock()
        self.loop = asyncio.get_event_loop()
        self.sock = rsock

        self.worker = EventSpoolWorker(delivery_stream, self.spool_dir, self.session, self.config, wsock, daemon=True)

    def __enter__(self):
        self.worker.start()
        util.close_all_socks(WSOCKS)
        self.loop.add_reader(self.sock.fileno(), self.read)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        logger.debug('Worker shutdown requested')
        while self.worker.is_alive():
            self.worker.terminate()
            self.worker.join(1)
        self.loop.remove_reader(self.sock.fileno())
        util.close_all_socks(RSOCKS)
        logger.debug('Worker shutdown complete')

    def read(self):
        try:
            buff = self.sock.recv(constant.MAX_MESSAGE_BUFFER)
            if buff:
                kwargs = msgpack.unpackb(buff, encoding='utf-8')
                if self.registry.active:
                    collector_name = kwargs.pop('name')
                    collector_op = kwargs.pop('op')
                    collector = self.registry.get(collector_name)
                    op = getattr(collector, collector_op)
                    op(**kwargs)
        except BlockingIOError:
            pass
        except UnpackValueError:
            logger.warn('Failed to unpack message', exc_info=True)

    def write(self, record):
        prefix = constant.TEMP_PREFIX + constant.SPOOL_PREFIX
        with NamedTemporaryFile(prefix=prefix, dir=self.spool_dir) as temp:
            logger.debug('Writing {0} byte record to {1}'.format(len(record), temp.name))
            temp.write(record)
            temp.flush()
            os.link(temp.name, temp.name.replace(constant.TEMP_PREFIX, ''))

    @classmethod
    def _validate_stream(cls, session, config, delivery_stream):
        client = session.client('firehose', config=config)
        response = client.describe_delivery_stream(DeliveryStreamName=delivery_stream)
        if not response['DeliveryStreamDescription']['DeliveryStreamStatus'] == 'ACTIVE':
            raise Exception('Firehose Delivery Stream is not active')


class EventSpoolWorker(Process):
    def __init__(self, delivery_stream, spool_dir, session, config, sock, *args, **kwargs):
        super(EventSpoolWorker, self).__init__(*args, **kwargs)
        self.delivery_stream = delivery_stream
        self.spool_dir = spool_dir
        self.session = session
        self.config = config
        self.sock = sock
        self.lock = threading.Lock()
        self.packer = msgpack.Packer(use_bin_type=True)
        self.flushed = 0

    def run(self):
        util.setproctitle('{0} ({1}:{2})'.format(__name__, self.delivery_stream, self.spool_dir))
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        util.close_all_socks(RSOCKS)
        self.loop = util.new_event_loop()
        self.loop.add_signal_handler(signal.SIGTERM, self.stop)
        self.schedule_flush()
        logger.debug('Worker starting')
        self.loop.run_forever()

        # Time passes...

        logger.debug('Worker shutting down')
        util.close_all_socks(WSOCKS)
        tasks = asyncio.gather(*asyncio.Task.all_tasks(loop=self.loop), loop=self.loop, return_exceptions=True)
        tasks.add_done_callback(lambda f: self.loop.stop())
        tasks.cancel()
        while not tasks.done() and not self.loop.is_closed():
            self.loop.run_forever()

        raise SystemExit(0)

    def stop(self):
        self.loop.stop()
        self.flush()

    def schedule_flush(self):
        self.loop.call_later(constant.TIMER_INTERVAL, self.flush_check)

    def flush_check(self):
        if not self.lock.locked():
            age = self.loop.time() - self.flushed
            batch_files = len(glob(os.path.join(self.spool_dir, constant.SPOOL_PREFIX) + '*'))

            labels = {'path': self.spool_dir}
            self.write_stats(name=constant.STAT_SPOOL_AGE, op='set', labels=labels, value=age)
            self.write_stats(name=constant.STAT_SPOOL_COUNT, op='set', labels=labels, value=batch_files)
            logger.debug('flush check: files={0} age={1}'.format(batch_files, age))
            if batch_files >= constant.MAX_BATCH_COUNT or age >= constant.FLUSH_TIME:
                self.loop.call_soon(self.flush)

        self.schedule_flush()

    def flush(self):
        with self.lock:
            record_files = glob(os.path.join(self.spool_dir, constant.SPOOL_PREFIX) + '*')
            while True:
                batch_kwargs = {'DeliveryStreamName': self.delivery_stream, 'Records': []}
                batch_size = 0
                batch_files = []
                labels = {'stream': self.delivery_stream}

                while record_files:
                    path = record_files.pop()
                    try:
                        file_size = os.path.getsize(path)
                        self.write_stats(name=constant.STAT_RECORD_BYTES, op='add', labels=labels, value=file_size)
                    except Exception as e:
                        logger.warn('Failed to get size of file {0} from spool: {1}'.format(path, e))
                        continue

                    if batch_size + file_size <= constant.MAX_BATCH_SIZE and len(batch_files) < constant.MAX_BATCH_COUNT:
                        logger.debug('Including {0} ({1} bytes) in batch'.format(path, file_size))
                        batch_size += file_size
                        batch_files.append(path)
                    else:
                        break

                for batch_file in batch_files:
                    try:
                        with open(batch_file, 'rb') as fh:
                            batch_kwargs['Records'].append({'Data': fh.read()})
                    except Exception as e:
                        logger.warn('Failed to read file {0} from spool: {1}'.format(batch_file, e))
                        break

                if batch_kwargs['Records']:
                    self.write_stats(name=constant.STAT_BATCH_RECORDS, op='add', labels=labels, value=len(batch_files))
                    self.write_stats(name=constant.STAT_BATCH_BYTES, op='add', labels=labels, value=batch_size)
                    logger.info('Batch has {0} bytes in {1} files'.format(batch_size, len(batch_files)))
                    try:
                        client = self.session.client('firehose', config=self.config)
                        response = client.put_record_batch(**batch_kwargs)
                    except Exception:
                        logger.error('Firehose put_record_batch failed', exc_info=True)
                        return

                    for (i, status) in enumerate(response['RequestResponses']):
                        if 'RecordId' in status:
                            logger.debug('Firehose record succeeded: {0}'.format(batch_files[i]))
                            try:
                                os.unlink(batch_files[i])
                            except Exception as e:
                                logger.warn('Failed to unlink successfully processed file {0} from spool: {1}'.format(batch_files[i], e))
                        else:
                            logger.warn('Firehose record failed: [{ErrorCode}] {ErrorMessage}'.format(**status))
                            labels['error_message'] = status['ErrorMessage']
                            self.write_stats(name=constant.STAT_BATCH_FAILED, op='add', labels=labels, value=1)
                            del labels['error_message']
                else:
                    logger.debug('Batch is empty')
                    self.flushed = self.loop.time()
                    return

    def write_stats(self, **kwargs):
        if not self.loop._stopping:
            task = self.loop.create_task(self._write_stats(**kwargs))
            task.add_done_callback(self._write_done)

    def _write_done(self, task):
        try:
            task.result()
        except CancelledError:
            pass
        except Exception as e:
            logger.warn('Error writing stats: {0}'.format(e))

    async def _write_stats(self, **kwargs):
        await self.loop.sock_sendall(self.sock, self.packer.pack(kwargs))
