import logging

import boto3
import botocore.exceptions
import botocore.utils
from pkg_resources import get_distribution

import ujson

logger = logging.getLogger(__name__)
pkgname = __name__.split('.')[0]
version = get_distribution(pkgname).version


def get_instance_region():
    fetcher = botocore.utils.InstanceMetadataFetcher()

    try:
        r = fetcher._get_request(
            url='http://169.254.169.254/latest/dynamic/instance-identity/document',
            needs_retry=fetcher._needs_retry_for_credentials
        )
        return ujson.loads(r.text).get('region', None)
    except botocore.utils._RetriesExceededError:
        logger.debug("Max number of attempts exceeded ({0}) when attempting to retrieve data from metadata service.".format(fetcher._num_attempts))


def get_region(region_name=None, profile_name=None):
    region = region_name or boto3.Session(profile_name=profile_name).region_name or get_instance_region()
    if not region:
        raise botocore.exceptions.NoRegionError
    return region


def send_http_ok(transport):
    transport.write('HTTP/1.1 200 OK\r\nServer: {}/{}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n'.format(pkgname, version).encode())


def send_http_stats(transport, stats):
    transport.write('HTTP/1.1 200 OK\r\nServer: {}/{}\r\nConnection: close\r\nContent-type: application/json\r\n\r\n'.format(pkgname, version).encode())
    transport.write(ujson.dumps(stats, transport, escape_forward_slashes=False, indent=2).encode())
