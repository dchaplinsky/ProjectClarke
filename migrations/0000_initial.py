from db.migration import BaseMigration


class Migration(BaseMigration):
    def forward(self):
        # just noop
        pass

    def backward(self):
        # just noop
        pass

    