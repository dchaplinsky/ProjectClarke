import re
import fnmatch
import os
from natsort import natsorted
from datetime import datetime
from collections import OrderedDict
import importlib.util
from .migration import BaseMigration


class MigrationManager:
    MIGRATION_TEMPLATE = """
from db.migration import BaseMigration


class Migration(BaseMigration):
    # define forward and backward methods below
    pass
    """

    def __init__(self, client, migrations_dir):
        self.client = client
        self.migrations_dir = migrations_dir
        self.migrations = {}

        self._load_from_db()
        self._load_from_fs()
        self._match_migrations()

    def _match_migrations(self):
        self._fs_migrations["0000_initial"].set_applied(True)

        for k, v in self._db_migrations.items():
            if k in self._fs_migrations:
                self._fs_migrations[k].set_applied(v)
            else:
                # TODO: exception?
                pass

        self.migrations = self._fs_migrations.copy()

    def _load_from_db(self):
        result = self.client.query("select from Migration")

        db_migrations = {}

        for r in result:
            db_migrations[r.name] = r.applied

        self._db_migrations = OrderedDict(
            [(k, db_migrations[k]) for k in natsorted(db_migrations.keys())]
        )

    @staticmethod
    def _load_python_module(path, module_name="foobar.foo"):
        spec = importlib.util.spec_from_file_location(module_name, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def _load_from_fs(self):
        fs_migrations = []

        for file in natsorted(os.listdir(self.migrations_dir)):
            if fnmatch.fnmatch(file, "*.py"):
                fname, _ = os.path.splitext(file)
                module = self._load_python_module(
                    os.path.join(self.migrations_dir, file),
                    "migrations.{}".format(fname),
                )
                assert issubclass(
                    module.Migration, BaseMigration
                ), "Migration should be subclassed from BaseMigration"
                fs_migrations.append(
                    (fname, module.Migration(name=fname, client=self.client))
                )

        self._fs_migrations = OrderedDict(fs_migrations)

    def _apply(self, migration):
        # TODO: ESCAPING!!!!

        # SHIT, no transactions for schema change/DML
        try:
            migration.forward()
            rec = self.client.command(
                "UPDATE Migration SET name='{}', applied=true UPSERT WHERE name='{}'".format(
                    migration.name, migration.name
                )
            )
        except:
            migration.backward()
            raise


    def _unapply(self, migration):
        # TODO: ESCAPING!!!!

        try:
            migration.backward()
            rec = self.client.command(
                "UPDATE Migration SET name='{}', applied=false UPSERT WHERE name='{}'".format(
                    migration.name, migration.name
                )
            )
        except:
            migration.forward()
            raise


    def generate_empty_migration(self, explainer=""):
        migration_names = list(self.fs_migrations.keys())
        if not migration_names:
            number = 1
        else:
            last_migration = migration_names[-1]
            m = re.match(r"(\d+)_", last_migration)
            assert m, "Cannot parse migration number from {}".format(last_migration)

            number = int(m.group(1)) + 1

        new_file_name = "{:04d}_{}_{:%Y%m%d_%H%M}.py".format(
            number, explainer or "auto", datetime.utcnow()
        )

        with open(os.path.join(self.migrations_dir, new_file_name), "w") as fp:
            fp.write(self.MIGRATION_TEMPLATE)

        return new_file_name

    def migrate(self, migration_name):
        latest_applied_migration = None

        keys = list(self.migrations.keys())

        if migration_name is None:
            migration_name = keys[-1]

        assert (
            migration_name in self.migrations
        ), "File with that migration cannot be found"

        for k, v in self.migrations.items():
            if v.applied:
                latest_applied_migration = k


        if latest_applied_migration is not None:
            pos_1 = keys.index(latest_applied_migration)
            pos_2 = keys.index(migration_name)

            if pos_1 < pos_2:
                for i in range(pos_2 + 1):
                    migration = self.migrations[keys[i]]
                    if not migration.applied:
                        print("Applying migration {}".format(migration.name))
                        self._apply(migration)

            elif pos_2 < pos_1:
                for i in range(pos_1, pos_2, -1):
                    migration = self.migrations[keys[i]]
                    if migration.applied:
                        print("Unapplying migration {}".format(migration.name))
                        migration.backward()
                        self._unapply(migration)
