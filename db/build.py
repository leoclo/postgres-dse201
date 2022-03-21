import pandas as pd
import os
import sys
sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
)
from db.schemas import schemas_ref, schemas_data
from src.db_mng import PostgresSql
from src.utils import utils


class BuildDb():

    def __init__(self, settings):
        self.settings = settings
        self.postgres_sql = PostgresSql(**settings['postgres'])
        self.gen_files = settings['setup']['generate_file']

    def __getitem__(self, k):
        return getattr(self, k)

    def build_schemas(self):
        for s in self.settings['setup']['schemas']:
            schema = schemas_ref[s]

            self.postgres_sql.drop_schema(schema['name'])
            self.postgres_sql.create_schema(schema['name'])

            for name, data in schema['tables'].items():
                ix = data.pop('index', {})
                self.postgres_sql.create_table(schema['name'], name, **data)
                if ix and self.settings['setup']['build_schemas']['ix']:
                    self.postgres_sql.create_index(schema['name'], name, **ix)

            if self.gen_files:
                with open(f'db/gen_files/{schema["name"]}.sql', 'w') as f:
                    f.write('\n\n'.join(self.postgres_sql.queries))
                    self.postgres_sql.queries = []

        return None

    def create_indexes(self):
        for s in self.settings['setup']['schemas']:
            schema = schemas_ref[s]
            for name, data in schema['tables'].items():
                ix = data.pop('index', {})
                if ix:
                    self.postgres_sql.create_index(schema['name'], name, **ix)

            if self.gen_files:
                with open(f'db/gen_files/{schema["name"]}_indexes.sql', 'w') as f:
                    f.write('\n\n'.join(self.postgres_sql.queries))
                    self.postgres_sql.queries = []

    def insert_data(self):
        for s in self.settings['setup']['data']:
            schema = schemas_data[s]
            for spec in schema['tables']:
                df = pd.read_csv(f'db/csv_data/{spec["file_name"]}.csv').rename(
                    columns=spec['rename_cols']
                ).drop_duplicates()

                for row in df.to_dict('records'):
                    self.postgres_sql.insert_row(
                        schema['name'], spec['table_name'], row.keys(),
                        row.values()
                    )
            if self.gen_files:
                with open(f'db/gen_files/{schema["name"]}_data.sql', 'w') as f:
                    f.write('\n\n'.join(self.postgres_sql.queries))
                    self.postgres_sql.queries = []

        return None

    def insert_alchemy(self):
        for s in self.settings['setup']['data']:
            schema = schemas_data[s]
            for spec in schema['tables']:
                df = pd.read_csv(f'db/csv_data/{spec["file_name"]}.csv').rename(
                    columns=spec['rename_cols']
                ).drop_duplicates()

                res = self.postgres_sql.insert_with_alchemy(
                    schema['name'], spec['table_name'], df
                )
                print(res)


def setup_schemas(settings, action):
    build_db = BuildDb(settings)
    try:
        build_db[action]()
    except Exception as e:
        utils.pretty_print(e.args)
        if len(build_db.postgres_sql.queries):
            print(build_db.postgres_sql.queries[-1])


    return None




if __name__ == '__main__':
    setup_schemas(utils.get_settings(), sys.argv[2])



