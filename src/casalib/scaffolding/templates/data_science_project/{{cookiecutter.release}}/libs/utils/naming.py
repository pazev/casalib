from dataclasses import dataclass
from unidecode import unidecode


@dataclass
class TableNameGenerator:
    """ Class to generate the table name """
    prefix: str

    def norm_(self, name: str) -> str:
        """ Normalize the given string """
        return unidecode(str(name)).replace(' ', '').lower()

    def make_name(
        self,
        domain: str,
        content: str,
        key_type: str,
        table_type: str,
    ):
        """ Generate the table name """
        return "___".join(
            map(
                self.norm_,
                [self.prefix, domain, content, key_type,
                 table_type
                 ]
            )
        )
