import asyncio
import logging
import signal

import boto3
import botocore.exceptions
import botocore.utils
from pkg_resources import get_distribution

import ujson

try:
    from setproctitle import setproctitle
except ImportError:
    def setproctitle(title):
        pass

logger = logging.getLogger(__name__)
pkgname = __name__.split('.')[0]
version = get_distribution(pkgname).version


def get_instance_region():
    fetcher = botocore.utils.InstanceMetadataFetcher()

    try:
        r = fetcher._get_request(
            url_path='/latest/dynamic/instance-identity/document',
            retry_func=fetcher._needs_retry_for_credentials
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


def new_event_loop():
    old_loop = asyncio.get_event_loop()
    new_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(new_loop)
    old_loop.stop()
    old_loop.close()
    return new_loop


def close_all_socks(socklist):
    for sock in socklist[:]:
        socklist.remove(sock)
        sock.close()


def interrupt(sig_in, stack):
    if sig_in == signal.SIGCHLD:
        raise ChildProcessError('Received SIGCHLD')
    elif sig_in == signal.SIGTERM:
        raise SystemExit('Received SIGTERM')
    else:
        logger.warn('Received unhandled signal {0}'.format(sig_in))
