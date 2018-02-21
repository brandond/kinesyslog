import logging
import signal
from asyncio import CancelledError, Task, gather, get_event_loop
from functools import partial
from tempfile import gettempdir

import click

from . import proxy
from .message import GelfMessage, SyslogMessage
from .server import (DatagramGelfServer, DatagramSyslogServer, GelfServer,
                     SecureGelfServer, SecureSyslogServer, SyslogServer)
from .sink import MessageSink
from .spool import EventSpool


def shutdown_exception_handler(loop, context):
    if "exception" not in context or not isinstance(context["exception"], CancelledError):
        loop.default_exception_handler(context)


@click.option(
    '--debug',
    is_flag=True,
    help='Enable debug logging to STDERR.',
)
@click.option(
    '--gelf',
    is_flag=True,
    help='Listen for messages in Graylog Extended Log Format (GELF) instead of Syslog.',
)
@click.option(
    '--profile',
    type=str,
    help='Use a specific profile from your credential file.',
)
@click.option(
    '--region',
    type=str,
    help='The region to use. Overrides config/env settings.',
)
@click.option(
    '--spool-dir',
    type=click.Path(exists=True, writable=True, file_okay=False, resolve_path=True),
    help='Spool directory for compressed records prior to upload.',
    default=gettempdir(),
    show_default=True,
)
@click.option(
    '--proxy-protocol',
    is_flag=True,
    help='Enable Proxy Protocol v1/v2 support for TCP and TLS listeners.',
)
@click.option(
    '--udp-port',
    type=int,
    help='Bind port for UDP listener; 0 to disable.',
    default=0,
    show_default=True,
    multiple=True,
)
@click.option(
    '--tcp-port',
    type=int,
    help='Bind port for TCP listener; 0 to disable.',
    default=0,
    show_default=True,
    multiple=True,
)
@click.option(
    '--key',
    type=click.Path(exists=True, readable=True, dir_okay=False, resolve_path=True),
    help='Private key file for TLS listener.',
    default='localhost.key',
    show_default=True,
)
@click.option(
    '--cert',
    type=click.Path(exists=True, readable=True, dir_okay=False, resolve_path=True),
    help='Certificate file for TLS listener.',
    default='localhost.crt',
    show_default=True,
)
@click.option(
    '--port',
    type=int,
    help='Bind port for TLS listener; 0 to disable.',
    default=6514,
    show_default=True,
    multiple=True,
)
@click.option(
    '--address',
    type=str,
    help='Bind address.',
    default='0.0.0.0',
    show_default=True,
)
@click.option(
    '--stream',
    type=str,
    help='Kinesis Firehose Delivery Stream Name.',
    required=True,
)
@click.command(short_help='List for incoming Syslog messages and submit to Kinesis Firehose')
def listen(**args):
    logging.basicConfig(level='INFO', format='%(asctime)-15s %(levelname)s:%(name)s %(message)s')
    loop = get_event_loop()
    loop.set_exception_handler(shutdown_exception_handler)

    if args.get('gelf', False):
        message_class = GelfMessage
        TLS = SecureGelfServer
        TCP = GelfServer
        UDP = DatagramGelfServer
    else:
        message_class = SyslogMessage
        TLS = SecureSyslogServer
        TCP = SyslogServer
        UDP = DatagramSyslogServer

    if args.get('proxy_protocol', False):
        TLS = proxy.wrap(TLS)
        TCP = proxy.wrap(TCP)

    if args.get('debug', False):
        logging.getLogger('kinesyslog').setLevel('DEBUG')
        logging.getLogger('asyncio').setLevel('INFO')
        loop.set_debug(True)
    else:
        logging.getLogger('botocore').setLevel('ERROR')

    servers = []
    try:
        if args.get('port', None):
            for port in args['port']:
                servers.append(TLS(host=args['address'], port=port, certfile=args['cert'], keyfile=args['key']))
        if args.get('tcp_port', None):
            for port in args['tcp_port']:
                servers.append(TCP(host=args['address'], port=port))
        if args.get('udp_port', None):
            for port in args['udp_port']:
                servers.append(UDP(host=args['address'], port=port))
    except Exception:
        logging.error('Failed to start server', exc_info=True)

    if not servers:
        return

    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, signame), partial(loop.stop))

    try:
        with EventSpool(delivery_stream=args['stream'], spool_dir=args['spool_dir']) as spool:
            with MessageSink(spool=spool, message_class=message_class) as sink:
                for server in servers:
                    loop.run_until_complete(server.start_server(sink=sink))
                loop.run_forever()
    except KeyboardInterrupt:
        tasks = gather(*Task.all_tasks(loop=loop), loop=loop, return_exceptions=True)
        tasks.add_done_callback(partial(loop.stop))
        tasks.cancel()
        while not tasks.done() and not loop.is_closed():
            loop.run_forever()
    finally:
        loop.close()
