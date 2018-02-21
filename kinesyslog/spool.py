import logging
import os
import time
from glob import glob
from multiprocessing import Lock
from tempfile import NamedTemporaryFile

from boto3 import Session
from botocore.config import Config

from .util import get_region

FLUSH_TIME = 60
MAX_REQUEST_SIZE = 1024 * 1024 * 4
MAX_RECORD_COUNT = 500
logger = logging.getLogger(__name__)
lock = Lock()


class EventSpool(object):
    __slots__ = ['delivery_stream', 'spool_dir', 'profile_name', 'config', 'flushed']
    PREFIX = 'firehose_event-'

    def __init__(self, delivery_stream, spool_dir, region_name=None, profile_name=None):
        self.delivery_stream = delivery_stream
        self.spool_dir = spool_dir
        self.profile_name = profile_name
        self.config = Config(retries={'max_attempts': 10}, region_name=get_region(region_name, profile_name))
        self.flushed = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.flush()

    def write(self, record):
        with NamedTemporaryFile(prefix=self.PREFIX, dir=self.spool_dir, delete=False) as temp:
            logger.info('Writing {0} byte record to {1}'.format(len(record), temp.name))
            temp.write(record)
        if time.time() - self.flushed >= FLUSH_TIME:
            self.flush()

    def flush(self):
        if lock.acquire(block=False):
            try:
                while True:
                    record_files = glob(os.path.join(self.spool_dir, self.PREFIX) + '*')
                    request_kwargs = {'DeliveryStreamName': self.delivery_stream, 'Records': []}
                    request_size = 0
                    request_files = []

                    for path in record_files:
                        file_size = os.path.getsize(path)
                        if request_size + file_size <= MAX_REQUEST_SIZE and len(request_files) < MAX_RECORD_COUNT:
                            logger.debug('Including {0} ({1} bytes) in batch'.format(path, file_size))
                            request_size += file_size
                            request_files.append(path)
                        else:
                            break

                    for request_file in request_files:
                        try:
                            with open(request_file, 'rb') as fh:
                                request_kwargs['Records'].append({'Data': fh.read()})
                        except Exception:
                            logger.error('Failed to read file {0} from spool'.format(request_file), exc_info=True)
                            return

                    if request_kwargs['Records']:
                        logger.info('Batch has {0} bytes in {1} files'.format(request_size, len(request_files)))
                        try:
                            session = Session(profile_name=self.profile_name)
                            client = session.client('firehose', config=self.config)
                            response = client.put_record_batch(**request_kwargs)
                        except Exception:
                            logger.error('Firehose put_record_batch failed', exc_info=True)
                            return

                        for (i, status) in enumerate(response['RequestResponses']):
                            if 'RecordId' in status:
                                logger.debug('Firehose record succeeded: {0}'.format(request_files[i]))
                                try:
                                    os.unlink(request_files[i])
                                except Exception:
                                    logger.error('Failed to unlink successfully processed record: {0}'.format(request_files[i]))
                            else:
                                logger.warning('Firehose record failed: [{ErrorCode}] {ErrorMessage}'.format(**status))
                    else:
                        logger.debug('Batch is empty')
                        return
            finally:
                self.flushed = time.time()
                lock.release()
