import logging
import re
from datetime import datetime, timedelta

import ujson

from .util import random_digits

logger = logging.getLogger(__name__)
epoch = datetime.utcfromtimestamp(0)
prio_pattern = re.compile(r"""^<(?P<prio>\d{1,3})>
                                (?P<content>.*)""", re.VERBOSE)
syslog_pattern = re.compile(r"""^<(?P<prio>\d{1,3})>
                               (?:(?P<rfc5424>1\ )?
                                  (?P<timestamp>\S{20,38}|...\ ..\ ..:..:..(\ 20\d\d)?|-))\ ?
                                  (?P<hostname>\S+)*\ ?
                                  (?P<content>.*)""", re.VERBOSE | re.IGNORECASE)


def parse_rfc3164_timestamp(timestamp):
    if len(timestamp) <= 15:
        timestamp = '{0} {1}'.format(timestamp, datetime.today().year)
    parsed_time = datetime.strptime(timestamp, '%b %d %H:%M:%S %Y')

    # If the timetamp is more than 2 days in the future, it's probably from last year
    if (parsed_time - datetime.now()).days > 2:
        parsed_time = parsed_time.replace(year=parsed_time.year - 1)

    return parsed_time


def parse_rfc5424_timestamp(timestamp):
    if timestamp[-1] == 'Z':
        return parse_rfc5424_date(timestamp[:-1])
    else:
        value = parse_rfc5424_date(timestamp[:-6])
        hours = int(timestamp[-5:-3])
        mins = int(timestamp[-2:])
        sign = 1 if timestamp[-6] == '-' else -1
        return value + timedelta(seconds=sign*(hours*3600+mins*60))


def parse_rfc5424_date(timestamp):
    if '.' in timestamp:
        return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")
    else:
        return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")


def format_rfc5424_date(timestamp):
    return datetime.strftime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")


def assign_uuid(message, timestamp):
    if isinstance(timestamp, datetime):
        timestamp = (timestamp - epoch).total_seconds()

    return {'id': random_digits(length=56),
            'message': message,
            'timestamp': int(round(timestamp * 1000)),
            }


class BaseMessage(object):
    name = 'base'

    @classmethod
    def create_events(cls, source, messages):
        for message, recv_ts in messages:
            yield cls.create_event(source, message, recv_ts)

    @classmethod
    def create_event(cls, source, message, recv_ts):
        raise NotImplementedError


class GelfMessage(BaseMessage):
    name = 'gelf'

    @classmethod
    def create_event(cls, source, message, recv_ts):
        message = message.decode('utf-8', 'backslashreplace')
        try:
            timestamp = ujson.loads(message).get('timestamp', recv_ts)
        except Exception:
            timestamp = datetime.utcfromtimestamp(recv_ts)

        return assign_uuid(message, timestamp)


class SyslogMessage(BaseMessage):
    name = 'syslog'

    @classmethod
    def create_event(cls, source, message, recv_ts):
        message = message.decode('utf-8', 'backslashreplace')
        parts = cls.get_message_header(source, message)
        try:
            timestamp = parse_rfc5424_timestamp(parts['timestamp']) if parts['rfc5424'] else parse_rfc3164_timestamp(parts['timestamp'])
        except Exception:
            timestamp = None

        if not parts['prio']:
            parts['prio'] = '13'

        if not isinstance(timestamp, datetime):
            timestamp = datetime.utcfromtimestamp(recv_ts)
            message = '<{0}>1 {1} {2} {3}'.format(parts['prio'], format_rfc5424_date(timestamp), source, parts['content'])

        return assign_uuid(message, timestamp)

    @classmethod
    def get_message_header(cls, source, message):
        for pattern in [syslog_pattern, prio_pattern]:
            match = pattern.match(message)
            if match:
                return match.groupdict()
        return {
            'prio': None,
            'rfc5242': None,
            'timestamp': None,
            'hostname': None,
            'content': message,
            }
