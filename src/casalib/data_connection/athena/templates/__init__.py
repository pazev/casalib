import jinja2

from . import create_table
from . import insert_table


env = jinja2.Environment()


templates_dict = {
    'create_table': env.from_string(create_table.TEMPLATE),
    'insert_table': env.from_string(insert_table.TEMPLATE),
}
