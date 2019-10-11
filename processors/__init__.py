from db.constants import NAME_TYPES
from collections import defaultdict, Counter
from names_translator.name_utils import parse_fullname, try_to_fix_mixed_charset, is_cyr


class AbstractNameProcessor:
    def __init__(self, language, nomenclature):
        self.language = language
        self.nomenclature = nomenclature

    def normalize_name(self, name):
        name = (
            name.replace('"', "")
            .replace("'", "")
            .replace("`", "")
            .replace("’", "")
            .replace("ʼ", "")
        )

        return name

    def digest(self, name):
        raise NotImplementedError()

    def from_iterable(self, it):
        for name in it:
            self.digest(name)

    def to_iterable(self, it):
        raise NotImplementedError()


class StatsNameProcessor(AbstractNameProcessor):
    def __init__(self, language, source, nomenclature, name_type="fullname"):
        super().__init__(language, nomenclature)
        self.name_type = name_type
        self.source = source
        self.freq_dicts = defaultdict(Counter)

    def digest(self, name):
        if self.language in ["uk, ru"]:
            name = try_to_fix_mixed_charset(name)

            if not is_cyr(name):
                return

        if self.name_type == "fullname":
            if self.language in ["uk", "ru"]:
                l, f, p, _ = parse_fullname(name)

                self.freq_dicts["firstname"].update([f.lower()])
                self.freq_dicts["lastname"].update([l.lower()])
                self.freq_dicts["patronymic"].update([p.lower()])
            else:
                pass
        else:
            self.freq_dicts[self.name_type].update([name.lower().strip()])

    def to_iterable(self):
        for name_type, v in self.freq_dicts.items():
            total = sum(v.values())
            classes = len(v)

            for name, occs in v.most_common():
                if not name.strip():
                    continue

                yield {
                    "name_type": name_type,
                    "languages": self.language,
                    "sources": self.source,
                    "nomenclatures": self.nomenclature,
                    "name": name,
                    "gender": "unk",
                    "normalized_name": self.normalize_name(name),
                    "frequencies": {
                        self.nomenclature: {
                            "classes": classes,
                            "occurences": occs,
                            "total": total,
                        }
                    },
                }
