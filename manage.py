#!/usr/bin/env python

import click
import os.path
from db.api import OrientDatabase


@click.group()
@click.argument("dsn", envvar="PYORIENT_DSN")
@click.pass_context
def cli(ctx, dsn):
    ctx.ensure_object(dict)
    ctx.obj["db_wrapper"] = OrientDatabase(
        dsn, os.path.join(os.path.dirname(__file__), "migrations")
    )


@cli.command()
@click.pass_context
def initdb(ctx):
    db_wrapper = ctx.obj["db_wrapper"]
    click.echo("Connected to server")
    click.echo("Connected to database")


@cli.command()
@click.pass_context
def showmigrations(ctx):
    db_wrapper = ctx.obj["db_wrapper"]

    for m in db_wrapper.migration_manager.migrations.values():
        click.echo(m)


@cli.command()
@click.pass_context
def makemigrations(ctx):
    db_wrapper = ctx.obj["db_wrapper"]
    click.echo(
        "Meet your new migration at migrations/{}".format(
            db_wrapper.migration_manager.generate_empty_migration()
        )
    )


@cli.command()
@click.pass_context
@click.argument("migration_name", required=False)
def migrate(ctx, migration_name):
    db_wrapper = ctx.obj["db_wrapper"]
    db_wrapper.migration_manager.migrate(migration_name)


if __name__ == "__main__":
    cli()
