class BaseMigration():
    def __init__(self, name, client):
        self.client = client
        self.applied = False
        self.name = name

    def set_applied(self, value):
        self.applied = value

    def forward(self):
        raise NotImplementedError()

    def backward(self):
        raise NotImplementedError()

    def __str__(self):
        return "[{}] {}".format("X" if self.applied else " ", self.name)