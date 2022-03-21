import pandas as pd
import psycopg2
import sqlalchemy


class PostgresSql():
    """
        A class that is used to comunicate with postgres database

        Parameters
        ----------

        host : str:
            host where the Postgres database server is located.
        port : int:
            Postgres port to use
        user : str
            Postgres user to log in as
        pwd : str
            Postgres user password
        db : str
            Database to use
    """

    def __init__(self, host, port, user, pwd, db):
        self.conn = psycopg2.connect(
            host=host, port=port, user=user, password=pwd, database=db
        )
        self.engine = sqlalchemy.create_engine(
            f'postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}'
        )

        self.queries = []

    @classmethod
    def from_settings(cls, settings):
        return cls(**settings['postgres'])

    def perform_query(self, qry):
        qry = qry.strip() + ';'
        self.queries.append(qry)
        with self.conn.cursor() as cursor:
            #db_res - int of items impacted
            db_res = cursor.execute(qry)
        self.conn.commit()
        return db_res

    def read_qry(self, qry):
        qry = qry.strip() + ';'
        self.queries.append(qry)
        return pd.read_sql(qry, self.engine)

    def analyze_read_qry(self, qry):
        qry = f'EXPLAIN ANALYZE {qry.strip()};'

        self.queries.append(qry)
        with self.conn.cursor() as cursor:
            cursor.execute(qry)
            return cursor.fetchall()

    def insert_with_alchemy(self, schema, table_name, df):
        return df.to_sql(
            table_name, self.engine, schema, if_exists='append', index=False
        )

    def create_schema(self, name):
        return self.perform_query(f'CREATE SCHEMA IF NOT EXISTS {name}')

    def drop_schema(self, name):
        return self.perform_query(f'DROP SCHEMA IF EXISTS {name} CASCADE')

    def create_table(self, schema, name, columns, constraints=[]):
        """
            Creates postgres table in a schema
        """

        cols_qry = ', '.join([f"{v['name']} {v['type']}" for v in columns])

        c_qry = ', '.join([
            f"CONSTRAINT {v['name']} {v['type']} REFERENCES {schema}.{v['spec']}"
            for v in constraints
        ])

        if len(c_qry):
            cols_qry += ','

        return self.perform_query(
            f'CREATE TABLE {schema}.{name}({cols_qry} {c_qry})'
        )

    def create_index(self, schema, table_name, name, cols, i_type='INDEX'):
        """ Creates index on table """

        return self.perform_query(f"""
            CREATE {i_type} {name} ON {schema}.{table_name}({", ".join(cols)})
        """)

    def insert_row(self, schema, table_name, columns, values):
        """ Creates index on table """

        cols = ', '.join(columns)
        vals = ', '.join([
            f"'{v}'" if isinstance(v, str) else str(v) for v in values
        ])

        return self.perform_query(f"""
            INSERT INTO {schema}.{table_name}({cols}) VALUES ({vals})
        """)



