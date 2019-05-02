import logging

import click

from . import init, util


def _print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo('{} {}'.format(util.pkgname, util.version))
    ctx.exit()


@click.group(context_settings={'max_content_width': 160})
@click.option(
    '--version',
    is_flag=True,
    callback=_print_version,
    expose_value=False,
    is_eager=True,
    help='Show current tool version.'
)
def cli():
    pass


logging.basicConfig(level='INFO', format='[%(process)d]<%(levelname)s> %(name)s: %(message)s')
cli.add_command(init.listen)
cli.add_command(init.install)

if __name__ == '__main__':
    cli()
