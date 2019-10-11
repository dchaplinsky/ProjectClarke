import os.path
from csv import DictReader, reader
import json

class Loader():
    def __init__(self, source, filetype):
        fname, fext = os.path.splitext(source)

        self.handler = None
        fext = fext.lstrip(".")
        if fext in ["gz", "bz2"]:
            if fext == "gz":
                self.handler = gzip.open(source, "wt")
            elif fext == "bz2":
                self.handler = bz2.open(source, "wt")

            _, fext = os.path.splitext(fname)
        else:
            self.handler = open(source, "r")

        if filetype == "auto":
            ext_mapping = {
                "csv": "csv",
                "json": "json",
                "jsonlines": "jsonlines",
                "txt": "text",
            }

            if fext.lower() not in ext_mapping:
                raise InvalidFileTypeException(
                    "Cannot recognize filetype, allowed filetypes are {}".format(
                        ", ".join(ext_mapping.keys())
                    )
                )
            else:
                self.filetype = ext_mapping[fext.lower()]
        else:
            self.filetype = filetype

    def iter_file(self):
        if self.filetype == "csv":
            r = DictReader(self.handler)
            for l in r:
                yield l
        elif self.filetype == "headless_csv":
            r = reader(self.handler)
            for l in r:
                yield l
        elif self.filetype == "jsonlines":
            for l in self.handler:
                yield json.loads(l)
        elif self.filetype == "json":
            data = json.load(self.handler)
            for l in data:
                yield l
        elif self.filetype == "text":
            for l in self.handler:
                yield [l.strip()]
