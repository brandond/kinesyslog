import logging
import os
import posix
import signal
import sys
from asyncio import CancelledError, Task, gather, get_event_loop
from pwd import getpwnam
from tempfile import gettempdir
from textwrap import dedent

import click

logger = logging.getLogger(__name__)


def shutdown_exception_handler(loop, context):
    if "exception" not in context or not isinstance(context["exception"], CancelledError):
        loop.default_exception_handler(context)


def validate_user(ctx, param, value):
    if value:
        try:
            return getpwnam(value).pw_name
        except KeyError as e:
            raise click.BadParameter(e)


@click.option(
    '--debug',
    is_flag=True,
    help='Enable debug logging to STDERR.',
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
    loop = get_event_loop()
    loop.set_exception_handler(shutdown_exception_handler)

    if kwargs.get('debug', False):
        logging.getLogger('kinesyslog').setLevel('DEBUG')
        logging.getLogger('asyncio').setLevel('INFO')
        loop.set_debug(True)
    else:
        logging.getLogger('botocore').setLevel('ERROR')

    from . import proxy
    from .message import GelfMessage, SyslogMessage
    from .server import (DatagramGelfServer, DatagramSyslogServer, GelfServer,
                         SecureGelfServer, SecureSyslogServer, SyslogServer)
    from .sink import MessageSink
    from .spool import EventSpool

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

    servers = []
    try:
        for port in kwargs['udp_port']:
            if port:
                servers.append(UDP(host=kwargs['address'], port=port))
        for port in kwargs['tcp_port']:
            if port:
                server = proxy.wrap(TCP) if port in kwargs['proxy_protocol'] else TCP
                servers.append(server(host=kwargs['address'], port=port))
        for port in kwargs['tls_port']:
            if port:
                server = proxy.wrap(TLS) if port in kwargs['proxy_protocol'] else TLS
                servers.append(server(host=kwargs['address'], port=port, certfile=kwargs['cert'], keyfile=kwargs['key']))
    except Exception as e:
        logger.error('Failed to validate {0} configuration: {1}'.format(
            e.__traceback__.tb_next.tb_frame.f_code.co_names[1], e))

    if not servers:
        logger.error('No valid servers configured! You must enable at least one UDP, TCP, or TLS port.')
        sys.exit(posix.EX_CONFIG)

    try:
        with EventSpool(delivery_stream=kwargs['stream'], spool_dir=kwargs['spool_dir'],
                        region_name=kwargs['region'], profile_name=kwargs['profile']) as spool:
            with MessageSink(spool=spool, message_class=message_class, group_prefix=kwargs['group_prefix']) as sink:
                for server in servers[:]:
                    try:
                        loop.run_until_complete(server.start_server(sink=sink))
                    except Exception as e:
                        logger.error('Failed to start {}: {}'.format(server.__class__.__name__, e))
                        servers.remove(server)
                if servers:
                    logger.info('Successfully started {} listeners'.format(len(servers)))
                    loop.add_signal_handler(signal.SIGTERM, lambda: loop.stop())
                    loop.run_forever()
                else:
                    raise Exception('All servers failed.')
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error('Failed to start Kinesyslog: {0}'.format(e))
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
