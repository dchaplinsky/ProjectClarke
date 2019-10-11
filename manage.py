#!/usr/bin/env python

import click
import gzip
import bz2
import os.path
import jmespath
from tqdm import tqdm
from itertools import chain
from collections import defaultdict, Counter
from db.api import OrientDatabase
from db.constants import LANGUAGES, GENDERS, NAME_TYPES, SOURCES
from loader.constants import FILETYPES
from loader.exceptions import InvalidFileTypeException
from loader import Loader
from processors import StatsNameProcessor


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
@click.option("--language", type=click.Choice(LANGUAGES), required=True)
@click.option(
    "--name_type",
    type=click.Choice(NAME_TYPES + ["fullname"]),
    default="fullname",
    help="Type of the name we are loading. Passing fullname will enable name parsing",
)
@click.option("--gender", type=click.Choice(GENDERS), default="unk")
@click.option(
    "--filetype",
    type=click.Choice(FILETYPES),
    default="auto",
    help="Type of the file to parse, leave empty to guess it by file extension",
)
@click.option(
    "--field_path",
    help="JMESpath to use to pull info from a particular field or column",
    required=True,
)
@click.option(
    "--nomenclature", help="Name (sluggified) for this datasource", required=True
)
@click.option(
    "--source", help="Type of the source", type=click.Choice(SOURCES), default="dataset"
)
@click.argument("in_files", nargs=-1, type=click.Path())
def load_nomenclature(
    ctx,
    language,
    name_type,
    gender,
    in_files,
    filetype,
    source,
    field_path,
    nomenclature,
):
    db_wrapper = ctx.obj["db_wrapper"]
    accum = defaultdict(Counter)

    def flatten(val):
        if isinstance(val, list):
            if len(val) == 1:
                return v[0]
            elif len(val) == 0:
                return None
        return val

    # Quicker alternatives first
    if field_path.isdigit():
        field_path = int(field_path)
        extract = lambda x: x[field_path]
    elif not (("." in field_path) or ("[" in field_path) or ("]" in field_path)):
        extract = lambda x: x[field_path]
    else:
        field_path = jmespath.compile(field_path)
        extract = lambda x: flatten(field_path.search(x))

    processor = StatsNameProcessor(
        language=language, source=source, name_type=name_type, nomenclature=nomenclature
    )

    processor.from_iterable(
        tqdm(map(
            extract,
            chain.from_iterable(
                [Loader(source, filetype).iter_file() for source in in_files]
            ),
        ))
    )

    for name_entity in tqdm(processor.to_iterable()):
        db_wrapper.put_name(name_entity)


if __name__ == "__main__":
    cli()
