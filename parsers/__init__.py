from names_translator.name_utils import parse_fullname


class AbstractFullNameParser():
    def parse(self, full_name):
        raise NotImplementedError()


class SlavicFullNameParser(AbstractFullNameParser):
    def parse(self, full_name):
        chunks = parse_fullname(full_name)[:-1]