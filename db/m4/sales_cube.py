import os
import sys

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
)

from src.db_mng import PostgresSql
from src.utils import utils


class SalesCubeOptions():

    def __init__(self, settings):
        self.postgres_sql = PostgresSql(**settings['postgres'])

    def __getitem__(self, k):
        return getattr(self, k)

    def pcomp_table_sales_data(self):
        self.postgres_sql.perform_query('DROP TABLE IF EXISTS sales_cube.sales_data')
        return  self.postgres_sql.perform_query("""
            CREATE TABLE sales_cube.sales_data AS (
                SELECT s.customer_id, s.product_id, s.amount, pr.price, pr.category_id, cust.state_id
                FROM sales_cube.sales s
                INNER JOIN sales_cube.products pr ON (s.product_id = pr.id)
                INNER JOIN sales_cube.customers cust ON (s.customer_id = cust.id)
            )
        """)

    def pcomp_table_top20cc(self):
        self.postgres_sql.perform_query('DROP TABLE IF EXISTS sales_cube.top20cc')
        return  self.postgres_sql.perform_query("""
            CREATE TABLE sales_cube.top20cc AS (
                WITH top20cat AS (
                    SELECT category_id, ROW_NUMBER() OVER (ORDER BY SUM(price*amount) DESC) AS "rank",  SUM(price*amount) AS total_sales FROM sales_cube.sales_data
                    GROUP BY category_id  ORDER BY total_sales DESC LIMIT 20
                ),
                top20cust AS (
                    SELECT customer_id, ROW_NUMBER() OVER (ORDER BY SUM(price*amount) DESC) AS "rank", SUM(price*amount) AS total_sales FROM sales_cube.sales_data
                    GROUP BY customer_id  ORDER BY total_sales DESC LIMIT 20
                )
                SELECT customer_id, top20cust.rank as customer_rank, category_id, top20cat.rank as category_rank FROM top20cat, top20cust
        )
        """)

    def index_sales_data(self):
        return self.postgres_sql.perform_query("""
            CREATE INDEX index_precomp_sales_data ON sales_cube.sales_data(customer_id);
        """)

    def index_top20cc(self):
        return self.postgres_sql.perform_query("""
            CREATE INDEX index_precomp_top20cc ON sales_cube.top20cc(category_id);
        """)

    def top20revenue(self):
        return self.postgres_sql.analyze_read_qry("""
            SELECT sales_cube.top20cc.*, COALESCE(SUM(sales_cube.sales_data.amount), 0) AS quantity_sold, COALESCE(SUM(sales_cube.sales_data.price*sales_cube.sales_data.amount), 0) AS dollar_value
            FROM sales_cube.top20cc
                LEFT JOIN sales_cube.sales_data ON (
                    sales_cube.top20cc.customer_id = sales_cube.sales_data.customer_id AND sales_cube.top20cc.category_id = sales_cube.sales_data.category_id
                )
            GROUP BY sales_cube.top20cc.customer_id, sales_cube.top20cc.customer_rank, sales_cube.top20cc.category_id, sales_cube.top20cc.category_rank
        """)


def build_options(settings):
    sc_options = SalesCubeOptions(settings)

    options_titles = {
        'pcomp_table_sales_data': 'First Precomputed Table - sales_cube.sales_data',
        'pcomp_table_top20cc': 'Second Precomputed Table - top20cc',
        'index_sales_data': 'Index of First Precomputed Table - sales_cube.sales_data',
        'index_top20cc': 'Index of Second Precomputed Table - sales_cube.top20cc',
        'top20revenue': 'New top20revenue using precomputed tables'
    }

    options = [
        'pcomp_table_sales_data',
        'pcomp_table_top20cc',
        'index_sales_data',
        'index_top20cc',
        'top20revenue'
    ]

    for opt in options:
        sc_options.postgres_sql.queries.append(
            f'\n\n-- {options_titles[opt]}\n'
        )
        res = sc_options[opt]()
        if opt == 'top20revenue':
            sc_options.postgres_sql.queries.append(
               '\n'.join(['-- ' + v[0] for v in res[-2:]])
            )


    with open(f'db/m4/files/sales_cube.sql', 'w') as f:
        f.write('\n'.join(sc_options.postgres_sql.queries))

    print('Success')


if __name__ == '__main__':
    build_options(utils.get_settings())