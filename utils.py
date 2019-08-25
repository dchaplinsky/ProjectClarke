from unidecode import unidecode
from names_translator.name_utils import parse_fullname, is_cyr


def replace_apostrophes(s):
    return s.replace("`", "'").replace("’", "'").replace('"', "'")


def normalize_name(name):
    return unidecode(replace_apostrophes(s)).lower()

