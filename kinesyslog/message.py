import logging
import re
from datetime import datetime, timedelta

from libuuid import uuid4

logger = logging.getLogger(__name__)
header = re.compile('^<\\d{1,3}>(?:(?P<rfc5424>1 )?(?P<timestamp>\\S{20,38}|... .. ..:..:..|-)) ')


def create_events(messages):
    events = []
    for message in messages:
        event = create_event(message)
        if event is not None:
            events.append(event)
    return events


def create_event(message):
    try:
        message = message.decode()
        is_rfc5424, timestamp = get_message_header(message)
        timestamp = parse_rfc5424_timestamp(timestamp) if is_rfc5424 else parse_rfc3164_timestamp(timestamp)
        return {'id': str(uuid4()), 'timestamp': timestamp.strftime('%Y-%m-%dT%H:%M:%S.%fZ'), 'message': message}
    except Exception:
        logger.error('Failed to parse message', exc_info=True)


def get_message_header(message):
    is_rfc5424 = False
    timestamp = '-'
    match = header.match(message)
    if match:
        is_rfc5424 = match.group('rfc5424') is not None
        timestamp = match.group('timestamp')

    return (is_rfc5424, timestamp)


def parse_rfc3164_timestamp(timestamp):
    if timestamp[0] == '-':
        return datetime.now()

    current_year = datetime.today().year
    timestamp_with_year = '{0:d} {1}'.format(current_year, timestamp)
    # FIXME - if the parsed time is off from now by more than a few months,
    # the year might have rolled over between the time we got the message
    # and when we're parsing it. Subtract a year?
    return datetime.strptime(timestamp_with_year, '%Y %b %d %H:%M:%S')


def parse_rfc5424_timestamp(timestamp):
    if timestamp[0] == '-':
        return datetime.now()

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
