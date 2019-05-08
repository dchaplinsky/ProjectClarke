#!/usr/bin/env python

import click
from db.api import OrientDatabase


@click.group()
@click.argument("dsn", envvar="PYORIENT_DSN")
@click.pass_context
def cli(ctx, dsn):
    ctx.ensure_object(dict)
    ctx.obj["dsn"] = dsn


@cli.command()
@click.pass_context
def initdb(ctx):
    db_wrapper = OrientDatabase(ctx.obj["dsn"])
    click.echo("Connected to server")
    db_wrapper.ensure_db_from_dsn()
    click.echo("Connected to database")


if __name__ == "__main__":
    cli()
