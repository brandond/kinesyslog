#!/usr/bin/env python
from __future__ import division, print_function

import codecs
import logging
import sys
import tempfile
import zlib

import boto3
import click
import ujson as json

try:
    import urlparse
    writer = codecs.getwriter('utf8')
    sys.stdout = writer(sys.stdout)
except ImportError:
    import urllib.parse as urlparse

logger = logging.getLogger(sys.argv[0])


@click.command()
@click.option('--profile', help='Use a specific profile from your credential file.', default=None)
@click.option('--region', help='Region to act upon.', default=None)
@click.option('--s3-uri', help='S3 bucket URI', required=True)
@click.option('--group', help='Filter messages by Log Group prefix')
@click.option('--length', help='Filter messages by minimum length', default=0)
def cli(profile, region, s3_uri, group, length):
    """
    Extract log messages from kinesyslog or Cloudwatch Logs streamed to S3.
    """
    s3 = boto3.Session(profile_name=profile).client('s3', region_name=region)
    parsed_uri = urlparse.urlparse(s3_uri)

    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=parsed_uri.netloc, Prefix=parsed_uri.path[1:], Delimiter=''):
        for s3_object in page.get('Contents', []):
            logger.info('Getting parts from {} bytes of {}'.format(s3_object['Size'], s3_object['Key']))
            for part in get_object_parts(s3, page['Name'], s3_object['Key']):
                if group and not part['logGroup'].startswith(group):
                    continue
                for event in get_text_events(part, length):
                    sys.stdout.write(event)
                    sys.stdout.write('\n')


def get_object_parts(s3, bucket, key):
    temp = tempfile.SpooledTemporaryFile(1024*1024*5)
    s3.download_fileobj(Bucket=bucket, Key=key, Fileobj=temp)
    temp.seek(0, 0)

    decompress = zlib.decompressobj(32+15)
    json_bytes = bytearray()
    comp_len = 0
    json_len = 0
    total_comp_len = 0
    total_json_len = 0

    while True:
        buf = temp.read(1024 * 32)
        comp_len += len(buf)

        if buf:
            json_bytes.extend(decompress.decompress(buf))
            if decompress.unused_data:
                try:
                    yield json.loads(json_bytes.decode())
                except ValueError:
                    logger.warn('Skipping invalid JSON')
                temp.seek(-len(decompress.unused_data), 1)
                comp_len -= len(decompress.unused_data)
                json_len += len(json_bytes)
                logger.info('Decompression yielded {0:9d} -> {1:9d} : {2:5.1f}%'.format(comp_len, json_len, (comp_len / json_len) * 100.0))
                json_bytes = bytearray()
                total_comp_len += comp_len
                total_json_len += json_len
                comp_len = 0
                json_len = 0
                decompress = zlib.decompressobj(32+15)
        else:
            json_bytes.extend(decompress.flush())
            try:
                yield json.loads(json_bytes.decode())
            except ValueError:
                logger.warn('Skipping invalid JSON')
            json_len += len(json_bytes)
            logger.info('Decompression yielded {0:9d} -> {1:9d} : {2:5.1f}%'.format(comp_len, json_len, (comp_len / json_len) * 100.0))
            total_comp_len += comp_len
            total_json_len += json_len
            break

    logger.info('Decompression yielded {0:9d} -> {1:9d} : {2:5.1f}% (TOTAL)'.format(total_comp_len, total_json_len, (total_comp_len / total_json_len) * 100.0))


def get_text_events(message, length):
    logger.info('Yielding {} messages for account {} to {} from {}'.format(
        len(message['logEvents']), message['owner'], message['logGroup'], message['logStream']))
    for event in message['logEvents']:
        if len(event['message']) >= length:
            yield event['message'].strip()


if __name__ == '__main__':
    logging.basicConfig(level='INFO', format='[%(process)d]<%(levelname)s> %(name)s: %(message)s')
    cli()
