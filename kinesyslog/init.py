import logging
import os
import posix
import signal
import sys
from asyncio import CancelledError, Task, gather, get_event_loop
from contextlib import ExitStack
from pwd import getpwnam
from tempfile import gettempdir
from textwrap import dedent

import click

logger = logging.getLogger(__name__)


def shutdown_exception_handler(loop, context):
    if 'exception' not in context or not isinstance(context['exception'], CancelledError):
        loop.default_exception_handler(context)


def validate_user(ctx, param, value):
    if value:
        try:
            return getpwnam(value).pw_name
        except KeyError as e:
            raise click.BadParameter(e)


@click.option(
    '--debug-asyncio',
    is_flag=True,
    help='With --debug, enable debugging of asyncio. This significantly decreases performance.',
    envvar='KINESYSLOG_DEBUG_ASYNCIO',
)
@click.option(
    '--debug',
    is_flag=True,
    help='Enable debug logging.',
    envvar='KINESYSLOG_DEBUG',
)
@click.option(
    '--group-prefix',
    type=str,
    help='Use the specified LogGroup prefix.',
    envvar='KINESYSLOG_GROUP_PREFIX',
    default='/kinesyslog',
    show_default=True,
)
@click.option(
    '--gelf',
    is_flag=True,
    help='Listen for messages in Graylog Extended Log Format (GELF) instead of Syslog.',
    envvar='KINESYSLOG_GELF',
)
@click.option(
    '--profile',
    type=str,
    help='Use a specific profile from your credential file.',
    envvar='KINESYSLOG_PROFILE',
)
@click.option(
    '--region',
    type=str,
    help='The region to use. Overrides config/env settings.',
    envvar='KINESYSLOG_REGION',
)
@click.option(
    '--spool-dir',
    type=click.Path(exists=True, writable=True, file_okay=False, resolve_path=True),
    help='Spool directory for compressed records prior to upload.',
    envvar='KINESYSLOG_SPOOL_DIR',
    default=gettempdir(),
    show_default=True,
)
@click.option(
    '--proxy-protocol',
    type=int,
    help='Enable PROXY protocol v1/v2 support on the selected TCP or TLS port; 0 to disable. May be repeated.',
    envvar='KINESYSLOG_PROXY_PROTOCOL',
    default=[0],
    show_default=True,
    multiple=True,
)
@click.option(
    '--key',
    type=click.Path(exists=True, readable=True, dir_okay=False, resolve_path=True),
    help='Private key file for TLS listener.',
    envvar='KINESYSLOG_KEY',
)
@click.option(
    '--cert',
    type=click.Path(exists=True, readable=True, dir_okay=False, resolve_path=True),
    help='Certificate file for TLS listener.',
    envvar='KINESYSLOG_CERT',
)
@click.option(
    '--tls-port',
    type=int,
    help='Bind port for TLS listener; 0 to disable. May be repeated.',
    envvar='KINESYSLOG_TLS_PORT',
    default=[6514],
    show_default=True,
    multiple=True,
)
@click.option(
    '--tcp-port',
    type=int,
    help='Bind port for TCP listener; 0 to disable. May be repeated.',
    envvar='KINESYSLOG_TCP_PORT',
    default=[0],
    show_default=True,
    multiple=True,
)
@click.option(
    '--udp-port',
    type=int,
    help='Bind port for UDP listener; 0 to disable. May be repeated.',
    envvar='KINESYSLOG_UDP_PORT',
    default=[0],
    show_default=True,
    multiple=True,
)
@click.option(
    '--prometheus-port',
    type=int,
    help='Bind port for Prometheus statistics listener; 0 to disable. May be repeated.',
    envvar='KINESYSLOG_PROMETHEUS_PORT',
    default=[0],
    show_default=True,
    multiple=True,
)
@click.option(
    '--address',
    type=str,
    help='Bind address.',
    envvar='KINESYSLOG_ADDRESS',
    default='0.0.0.0',
    show_default=True,
)
@click.option(
    '--stream',
    type=str,
    help='Kinesis Firehose Delivery Stream Name.',
    envvar='KINESYSLOG_STREAM',
    required=True,
)
@click.command(short_help='List for incoming Syslog messages and submit to Kinesis Firehose')
def listen(**kwargs):
    if kwargs.get('debug', False):
        logging.getLogger('kinesyslog').setLevel('DEBUG')
        logging.getLogger('asyncio').setLevel('INFO')
        if kwargs.get('debug_asyncio', False):
            logging.getLogger('asyncio').setLevel('DEBUG')
            os.environ['PYTHONASYNCIODEBUG'] = '1'
    else:
        logging.getLogger('botocore').setLevel('ERROR')

    prometheus_loaded = False
    loop = get_event_loop()
    loop.set_exception_handler(shutdown_exception_handler)

    from . import proxy, util
    from .message import GelfMessage, SyslogMessage
    from .protocol import BaseLoggingProtocol
    from .server import (DatagramGelfServer, DatagramSyslogServer, GelfServer,
                         SecureGelfServer, SecureSyslogServer, SyslogServer)
    from .sink import MessageSink
    from .spool import EventSpoolReader, EventSpoolWriter
    from .util import create_registry

    if 0 not in kwargs.get('prometheus_port', [0]):
        try:
            from .prometheus import StatsServer, StatsSink, StatsRegistry
            prometheus_loaded = True
        except ImportError:
            pass
    if not prometheus_loaded:
        from .stats import StatsServer, StatsSink, StatsRegistry  # noqa F811

    if kwargs.get('gelf', False):
        message_class = GelfMessage
        TLS = SecureGelfServer
        TCP = GelfServer
        UDP = DatagramGelfServer
    else:
        message_class = SyslogMessage
        TLS = SecureSyslogServer
        TCP = SyslogServer
        UDP = DatagramSyslogServer

    registry = create_registry(StatsRegistry)
    servers = []
    try:
        for port in kwargs['prometheus_port']:
            if port:
                server = proxy.wrap(StatsServer) if port in kwargs['proxy_protocol'] else StatsServer
                servers.append(server(host=kwargs['address'], port=port, registry=registry))
        for port in kwargs['tls_port']:
            if port:
                server = proxy.wrap(TLS) if port in kwargs['proxy_protocol'] else TLS
                servers.append(server(host=kwargs['address'], port=port, registry=registry, certfile=kwargs['cert'], keyfile=kwargs['key']))
        for port in kwargs['tcp_port']:
            if port:
                server = proxy.wrap(TCP) if port in kwargs['proxy_protocol'] else TCP
                servers.append(server(host=kwargs['address'], port=port, registry=registry))
        for port in kwargs['udp_port']:
            if port:
                servers.append(UDP(host=kwargs['address'], port=port, registry=registry))
    except Exception as e:
        logger.error('Failed to validate {0} configuration: {1}'.format(
            e.__traceback__.tb_next.tb_frame.f_code.co_names[1], e))

    if servers:
        if registry.active:
            registry.get('kinesyslog_listener_count').set(labels={}, value=len(servers) - 1)
    else:
        logger.error('No valid servers configured -  you must enable at least one UDP, TCP, or TLS port')
        sys.exit(posix.EX_CONFIG)

    try:
        with ExitStack() as stack:
            spool_writer = EventSpoolWriter(spool_dir=kwargs['spool_dir'])
            spool_reader = EventSpoolReader(delivery_stream=kwargs['stream'],
                                            spool_dir=kwargs['spool_dir'],
                                            registry=registry,
                                            region_name=kwargs['region'],
                                            profile_name=kwargs['profile'])
            account = spool_reader.get_account()
            stack.enter_context(spool_reader)
            sinks = []

            for server in servers:
                sink = MessageSink if issubclass(server.PROTOCOL, BaseLoggingProtocol) else StatsSink
                sink = sink(spool=spool_writer,
                            server=server,
                            message_class=message_class,
                            group_prefix=kwargs['group_prefix'],
                            account=account)
                context = stack.enter_context(sink)
                sinks.append(context)

            for i, server in enumerate(servers):
                try:
                    loop.run_until_complete(server.start(sink=sinks[i], loop=loop))
                    util.setproctitle('{0} (master:{1})'.format(__name__, i+1))
                except Exception as e:
                    logger.error('Failed to start {}: {}'.format(server.__class__.__name__, e), exc_info=True)
                    servers.remove(server)

            if servers:
                # Everything started successfully, set up signal handlers and wait until termination
                try:
                    logger.info('Successfully started {} servers'.format(len(servers)))
                    signal.signal(signal.SIGTERM, util.interrupt)
                    signal.signal(signal.SIGCHLD, util.interrupt)
                    loop.run_forever()
                except (KeyboardInterrupt, ChildProcessError, SystemExit) as e:
                    signal.signal(signal.SIGTERM, signal.SIG_DFL)
                    signal.signal(signal.SIGCHLD, signal.SIG_DFL)
                    logger.info('Shutting down servers: {0}({1})'.format(e.__class__.__name__, e))

                # Time passes...

                for server in servers:
                    loop.run_until_complete(server.stop())
            else:
                raise Exception('All servers failed')
    except Exception as e:
        logger.error('Unhandled exception: {0}'.format(e))
        if isinstance(e, PermissionError):
            sys.exit(posix.EX_NOPERM)
        else:
            sys.exit(posix.EX_CONFIG)
    finally:
        tasks = gather(*Task.all_tasks(loop=loop), loop=loop, return_exceptions=True)
        tasks.add_done_callback(lambda f: loop.stop())
        tasks.cancel()
        while not tasks.done() and not loop.is_closed():
            loop.run_forever()


@click.option(
    '--system-dir',
    type=click.Path(exists=True, file_okay=False, dir_okay=True, writable=True),
    help='Install the service unit to the specified systemd system directory',
    default='/etc/systemd/system',
    show_default=True
)
@click.option(
    '--user',
    type=str,
    help='Configure the service unit to run as specified user. If not specified, the service runs as root.',
    callback=validate_user,
)
@click.command(short_help="Install a SystemD service unit")
def install(system_dir, user):
    unit_file = os.path.join(system_dir, 'kinesyslog.service')
    override_file = os.path.join(system_dir, 'kinesyslog.service.d', 'override.conf')

    click.echo('Installing service unit {0} with ExecStart="{1} listen"'.format(unit_file, sys.argv[0]))

    with open(unit_file, 'wb') as f:
        f.write(dedent("""
            [Unit]
            Description=kinesyslog
            After=network-online.target

            [Service]
            Type=simple
            ExecStart={0} listen
            WorkingDirectory={1}
            KillMode=process
            Restart=on-failure
            RestartSec=30sec
            PrivateTmp=true
            AmbientCapabilities=CAP_NET_BIND_SERVICE

            [Install]
            WantedBy=multi-user.target
            """).format(sys.argv[0], os.path.dirname(sys.argv[0])).encode())

    os.makedirs(os.path.dirname(override_file), exist_ok=True)
    click.echo()
    if os.path.exists(override_file):
        click.echo('Not replacing existing service unit configuration file at {0}'.format(override_file))
    else:
        click.echo('Run "systemctl edit kinesyslog" to configure options (listening ports, stream names, etc)')
        with open(override_file, 'wb') as f:
            f.write(dedent("""
                [Service]
                ###
                ### Uncomment the following variables to suit your environment.
                ### It is not necessary to double-quote values containing spaces
                ###

                ### Configure a proxy server. Note that these should be in lowercase, unlike most other variables.
                Environment="no_proxy=169.254.169.254"
                #Environment="all_proxy=proxy.example.com:8080"

                ### It is recommended that you create a non-root user for kinesyslog
                """).encode())

            if user:
                f.write(dedent("User={0}").format(user).encode())
            else:
                f.write(dedent("#User=kinesyslog").encode())

            f.write(dedent("""

                ### Specify credentials or override the default config and credential file paths
                #Environment="AWS_ACCESS_KEY_ID="
                #Environment="AWS_SECRET_ACCESS_KEY="
                #Environment="AWS_SHARED_CREDENTIALS_FILE="
                #Environment="AWS_CONFIG_FILE="
                #Environment="AWS_PROFILE="

                ### Kinesyslog Configuration Options
                """).encode())

            for p in listen.params:
                default = ' '.join(str(d) for d in p.default) if isinstance(p.default, list) else p.default
                default = '' if default in [False, None] else default
                f.write('#Environment="{0}={1}"\n'.format(p.envvar, default).encode())

    click.echo('Run "systemctl daemon-reload; systemctl start kinesyslog" to start the service"')
    click.echo()
