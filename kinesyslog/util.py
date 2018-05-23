import logging

import boto3
import botocore

import ujson

logger = logging.getLogger(__name__)


def get_instance_region():
    data = {}
    fetcher = botocore.utils.InstanceMetadataFetcher()

    try:
        r = fetcher._get_request('http://169.254.169.254/latest/dynamic/instance-identity/document', fetcher._timeout, fetcher._num_attempts)
        if r.content:
            val = r.content.decode('utf-8')
            if val[0] == '{':
                data = ujson.loads(val)
    except botocore.utils._RetriesExceededError:
        logger.debug("Max number of attempts exceeded ({0}) when attempting to retrieve data from metadata service.".format(fetcher._num_attempts))

    return data.get('region', None)


def get_region(region_name=None, profile_name=None):
    region = region_name or boto3.Session(profile_name=profile_name).region_name or get_instance_region()
    if not region:
        raise botocore.exceptions.NoRegionError
    return region
