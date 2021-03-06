import json
import pyorient
import dsnparse
from .migration_manager import MigrationManager


class OrientDatabase:
    def __init__(self, dsn, migrations_dir=None):
        r = dsnparse.parse(dsn)

        self.client = pyorient.OrientDB(r.host or "localhost", r.port or 2424)
        self.session_id = self.client.connect(r.user, r.password)

        self.db = None
        self.parsed_dsn = r

        self.migration_manager = None

        if self.parsed_dsn.dbname:
            self.ensure_db_from_dsn()

            if migrations_dir is not None:
                self.init_migration_manager(migrations_dir)

    def ensure_db_from_dsn(self):
        assert self.parsed_dsn.dbname, "You should specify database name"

        self.ensure_db(
            self.parsed_dsn.dbname,
            self.parsed_dsn.user,
            self.parsed_dsn.password,
            **self.parsed_dsn.query
        )

    def ensure_db(
        self,
        dbname,
        user,
        password,
        dbtype="DB_TYPE_GRAPH",
        dbstorage="STORAGE_TYPE_PLOCAL",
        **kwargs
    ):
        if not self.client.db_exists(dbname, getattr(pyorient, dbstorage)):
            self.client.db_create(
                dbname, getattr(pyorient, dbtype), getattr(pyorient, dbstorage)
            )
            self.init_migrations()

        self.db = self.client.db_open(dbname, user, password)

    def init_migrations(self):
        self.client.command("CREATE CLASS Migration")
        self.client.command("CREATE PROPERTY Migration.name STRING (MANDATORY TRUE)")
        self.client.command(
            "CREATE PROPERTY Migration.applied BOOLEAN (MANDATORY TRUE)"
        )

    def init_migration_manager(self, migrations_dir):
        self.migration_manager = MigrationManager(self.client, migrations_dir)

    def put_name(self, name_entity):
        put_clause = ""
        for k, v in name_entity.get("frequencies", {}).items():
            v.update(
                {"@type":"d", "@class":"Freq"}
            )
            put_clause += 'PUT frequencies = "{k}", {v}\n'.format(k=k, v=json.dumps(v))

        command = """
            UPDATE 
                Name
            SET 
                name="{name}",
                gender="{gender}",
                name_type="{name_type}",
                normalized_name="{normalized_name}"
            ADD
                languages="{languages}",
                sources="{sources}",
                nomenclatures="{nomenclatures}",
                sources="{sources}"
            {put_clause}
            UPSERT WHERE
                name="{name}" and
                gender="{gender}" and
                name_type="{name_type}"
            """.format(
                # frequencies_json=frequencies_json,
                put_clause=put_clause,
                **name_entity
            )

        rec = self.client.command(command)
