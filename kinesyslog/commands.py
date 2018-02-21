import click

from . import core
from pkg_resources import get_distribution


def _print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo(get_distribution(__name__.split('.')[0]).version)
    ctx.exit()


@click.group()
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


cli.add_command(core.listen)

if __name__ == '__main__':
    cli()
