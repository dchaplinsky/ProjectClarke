from db.migration import BaseMigration
from db.constants import LANGUAGES, GENDERS, NAME_TYPES, SOURCES, TRANSLATION_TYPES


class Migration(BaseMigration):
    # define forward and backward methods below

    def backward(self):
        self.client.command("DROP CLASS Name IF EXISTS")
        self.client.command("DROP CLASS Translation IF EXISTS")

    def forward(self):
        self.client.command("CREATE CLASS Name EXTENDS V")
        self.client.command("CREATE PROPERTY Name.name STRING (MANDATORY TRUE)")
        self.client.command(
            "CREATE PROPERTY Name.normalized_name STRING (MANDATORY TRUE)"
        )
        self.client.command(
            "CREATE PROPERTY Name.languages EMBEDDEDSET STRING (MANDATORY TRUE)"
        )
        self.client.command(
            "ALTER PROPERTY Name.languages REGEXP '{}'".format("|".join(LANGUAGES))
        )

        self.client.command("CREATE PROPERTY Name.gender STRING (MANDATORY TRUE)")
        self.client.command(
            "ALTER PROPERTY Name.gender REGEXP '{}'".format("|".join(GENDERS))
        )

        self.client.command("CREATE PROPERTY Name.name_type STRING (MANDATORY TRUE)")
        self.client.command(
            "ALTER PROPERTY Name.name_type REGEXP '{}'".format("|".join(NAME_TYPES))
        )

        self.client.command("CREATE PROPERTY Name.sources EMBEDDEDSET STRING")
        self.client.command(
            "ALTER PROPERTY Name.sources REGEXP '{}'".format("|".join(SOURCES))
        )

        self.client.command("CREATE PROPERTY Name.nomenclature EMBEDDEDSET STRING")

        self.client.command(
            "CREATE INDEX names on Name (name, gender, name_type) UNIQUE"
        )

        self.client.command("CREATE CLASS Translation EXTENDS E")
        self.client.command("CREATE PROPERTY Translation.type STRING (MANDATORY TRUE)")
        self.client.command(
            "ALTER PROPERTY Translation.type REGEXP '{}'".format(
                "|".join(TRANSLATION_TYPES)
            )
        )
