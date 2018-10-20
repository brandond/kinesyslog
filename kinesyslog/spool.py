import logging
import os
import signal
from glob import glob

from asyncio import get_event_loop, gather, Task
from multiprocessing import Process
from tempfile import NamedTemporaryFile

from boto3 import Session
from botocore.config import Config

from .util import get_region
from . import constant

logger = logging.getLogger(__name__)


class EventSpool(object):
    def __init__(self, delivery_stream, spool_dir, region_name=None, profile_name=None):
        self.session = Session(profile_name=profile_name)
        self.config = Config(retries={'max_attempts': 10}, region_name=get_region(region_name, profile_name))
        self._validate_stream(self.session, self.config, delivery_stream)

        self.spool_dir = spool_dir
        self.worker = EventSpoolWorker(delivery_stream, self.spool_dir, self.session, self.config, daemon=True)
        self.worker.start()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        logger.debug('Worker shutdown requested')
        while self.worker.is_alive():
            self.worker.terminate()
            self.worker.join(1)
        logger.debug('Worker shutdown complete')

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
    def __init__(self, delivery_stream, spool_dir, session, config, *args, **kwargs):
        super(EventSpoolWorker, self).__init__(*args, **kwargs)
        self.delivery_stream = delivery_stream
        self.spool_dir = spool_dir
        self.session = session
        self.config = config
        self.flushed = 0

    def run(self):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        self.loop = get_event_loop()
        self.loop.add_signal_handler(signal.SIGTERM, self.stop)
        self.schedule_flush()
        logger.debug('Worker starting')
        self.loop.run_forever()

        logger.debug('Worker shutting down')
        tasks = gather(*Task.all_tasks(loop=self.loop), loop=self.loop, return_exceptions=True)
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
        age = self.loop.time() - self.flushed
        batch_files = len(glob(os.path.join(self.spool_dir, constant.SPOOL_PREFIX) + '*'))

        logger.debug('flush check: files={0} age={1}'.format(batch_files, age))
        if batch_files >= constant.MAX_RECORD_COUNT or age >= constant.FLUSH_TIME:
            self.loop.call_soon(self.flush)
        self.schedule_flush()

    def flush(self):
        try:
            while True:
                record_files = glob(os.path.join(self.spool_dir, constant.SPOOL_PREFIX) + '*')
                batch_kwargs = {'DeliveryStreamName': self.delivery_stream, 'Records': []}
                batch_size = 0
                batch_files = []

                for path in record_files:
                    file_size = os.path.getsize(path)
                    if batch_size + file_size <= constant.FLUSH_SIZE and len(batch_files) < constant.MAX_RECORD_COUNT:
                        logger.debug('Including {0} ({1} bytes) in batch'.format(path, file_size))
                        batch_size += file_size
                        batch_files.append(path)
                    else:
                        break

                for batch_file in batch_files:
                    try:
                        with open(batch_file, 'rb') as fh:
                            batch_kwargs['Records'].append({'Data': fh.read()})
                    except Exception:
                        logger.error('Failed to read file {0} from spool'.format(batch_file), exc_info=True)
                        return

                if batch_kwargs['Records']:
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
                            except Exception:
                                logger.error('Failed to unlink successfully processed record: {0}'.format(batch_files[i]))
                        else:
                            logger.warning('Firehose record failed: [{ErrorCode}] {ErrorMessage}'.format(**status))
                else:
                    logger.debug('Batch is empty')
                    return
        finally:
            self.flushed = self.loop.time()
