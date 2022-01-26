import os
import sys
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
)
from db.schemas import schemas_ref
from src.db_mng import PostgresSql
from src.utils import utils


def build_schema(postgres_sql, schema, generate_file):
    postgres_sql.drop_schema(schema['name'])


    postgres_sql.create_schema(schema['name'])

    for name, data in schema['tables'].items():
        ix = data.pop('index', {})
        postgres_sql.create_table(schema['name'], name, **data)
        if ix:
            postgres_sql.create_index(schema['name'], name, **ix)

    if generate_file:
        with open(f'db/gen_files/{schema["name"]}.sql', 'w') as f:
            f.write('\n\n'.join(postgres_sql.queries))
            postgres_sql.queries = []

    return None



def setup_schemas(settings):
    postgres_sql = PostgresSql(**settings['postgres'])


    for s in settings['setup']['schemas']:
        schema = schemas_ref[s]
        try:
            build_schema(postgres_sql, schema, settings['setup']['gen_files'])

        except Exception as e:
            utils.pretty_print(e.args)
            print(postgres_sql.queries[-1])


    return None




if __name__ == '__main__':
    setup_schemas(utils.get_settings())


