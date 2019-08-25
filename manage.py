#!/usr/bin/env python

import click
import os.path
from collections import defaultdict, Counter
from db.api import OrientDatabase
from db.constants import LANGUAGES, GENDERS, NAME_TYPES


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


@cli.group("etl")
@click.pass_context
def etl(ctx):
    pass


@etl.command()
@click.pass_context
@click.argument('language', type=click.Choice(LANGUAGES))
@click.argument('name_type', type=click.Choice(NAME_TYPES + ["fullname"]), default="fullname")
@click.argument('gender', type=click.Choice(GENDERS), default="unk")
@click.option("--source")
def load_nomenclature(ctx, language, name_type, gender, source):
    db_wrapper = ctx.obj["db_wrapper"]
    accum = defaultdict(Counter)

    if name_type == "fullname":
        if language in ["uk", "ru"]:
            pass

    print(language, name_type, gender)


if __name__ == "__main__":
    cli()
