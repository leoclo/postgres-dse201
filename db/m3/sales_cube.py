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

    def total_sales(self):
        return self.postgres_sql.analyze_read_qry(
            f"""
                WITH sales_with_price AS (
                    SELECT s.customer_id, s.product_id, s.amount, pr.price
                    FROM sales_cube.sales s
                    INNER JOIN sales_cube.products pr ON (s.product_id = pr.id)
                )
                SELECT
                    cust.id as customer_id,
                    COALESCE(SUM(swp.amount), 0) AS total_quantity_sold,
                    COALESCE(SUM(swp.price*swp.amount), 0) AS total_dollar_value
                FROM sales_cube.customers cust
                    LEFT JOIN sales_with_price swp ON (cust.id = swp.customer_id)
                GROUP BY cust.id ORDER BY cust.id ASC
            """
        )

    def total_sales_states(self):
        return self.postgres_sql.analyze_read_qry(
            f"""
                WITH sales_with_price AS (
                    SELECT s.customer_id, s.product_id, s.amount, pr.price
                    FROM sales_cube.sales s
                    INNER JOIN sales_cube.products pr ON (s.product_id = pr.id)
                ),
                sales_by_cust AS (
                    SELECT
                        cust.id as customer_id,
                        cust.state_id,
                        COALESCE(SUM(swp.amount), 0) AS total_quantity_sold,
                        COALESCE(SUM(swp.price*swp.amount), 0) AS total_dollar_value
                    FROM sales_cube.customers cust
                        LEFT JOIN sales_with_price swp ON (cust.id = swp.customer_id)
                    GROUP BY cust.id ORDER BY cust.id ASC
                )
                SELECT
                    st.ab as state_ab,
                    COALESCE(SUM(sbc.total_quantity_sold), 0) AS total_quantity_sold,
                    COALESCE(SUM(sbc.total_dollar_value), 0) AS total_dollar_value
                    FROM sales_cube.states st
                    LEFT JOIN sales_by_cust sbc ON (st.id = sbc.state_id)
                GROUP BY st.ab
            """
        )

    def total_sales_product_customer(self):
        return self.postgres_sql.analyze_read_qry(
            f"""
                WITH sales_with_price AS (
                    SELECT s.customer_id, s.product_id, s.amount, pr.price
                    FROM sales_cube.sales s
                    INNER JOIN sales_cube.products pr ON (s.product_id = pr.id)
                )
                SELECT swp.product_id AS pid,
                    swp.customer_id,
                    SUM(swp.price*swp.amount) AS total
                FROM sales_with_price swp
                WHERE swp.customer_id = 1
                GROUP BY swp.customer_id, swp.product_id
                ORDER BY total DESC
            """
        )

    def total_sales_all_products_all_customers(self):
        return self.postgres_sql.analyze_read_qry(
            f"""
                WITH all_c_p AS (
                    SELECT cust.id as customer_id, pr.id as product_id
                    FROM  sales_cube.customers as cust, sales_cube.products pr
                ),
                all_c_p_amt AS (
                    SELECT
                        all_c_p.customer_id,
                        all_c_p.product_id,
                        COALESCE(SUM(sl.amount), 0) as amt
                    FROM all_c_p
                    LEFT JOIN sales_cube.sales sl ON (all_c_p.product_id = sl.product_id)
                    GROUP BY all_c_p.customer_id, all_c_p.product_id
                )

                SELECT all_c_p_amt.customer_id, all_c_p_amt.product_id, SUM(pr.price*all_c_p_amt.amt) AS total_sales
                FROM all_c_p_amt
                    LEFT JOIN sales_cube.products as pr ON (all_c_p_amt.product_id=pr.id)
                GROUP BY all_c_p_amt.customer_id, all_c_p_amt.product_id
                ORDER BY total_sales
            """
        )

    def total_sales_product_category_state(self):
        return self.postgres_sql.analyze_read_qry(
            f"""
                WITH sales_data AS (
                    SELECT s.customer_id, s.product_id, s.amount, pr.price, pr.category_id, cust.state_id
                    FROM sales_cube.sales s
                    INNER JOIN sales_cube.products pr ON (s.product_id = pr.id)
                    INNER JOIN sales_cube.customers cust ON (s.customer_id = cust.id)
                )
                SELECT category_id, state_id, SUM(price*amount) AS total_sales FROM sales_data
                GROUP BY category_id, state_id ORDER BY total_sales DESC
            """
        )

    def top20revenue(self):
        return self.postgres_sql.analyze_read_qry(
            f"""
                WITH sales_data AS (
                    SELECT s.customer_id, s.product_id, s.amount, pr.price, pr.category_id, cust.state_id
                    FROM sales_cube.sales s
                    INNER JOIN sales_cube.products pr ON (s.product_id = pr.id)
                    INNER JOIN sales_cube.customers cust ON (s.customer_id = cust.id)
                ),
                top20cat AS (
                    SELECT category_id, ROW_NUMBER() OVER (ORDER BY SUM(price*amount) DESC) AS "rank",  SUM(price*amount) AS total_sales FROM sales_data
                    GROUP BY category_id  ORDER BY total_sales DESC LIMIT 20
                ),
                top20cust AS (
                    SELECT customer_id, ROW_NUMBER() OVER (ORDER BY SUM(price*amount) DESC) AS "rank", SUM(price*amount) AS total_sales FROM sales_data
                    GROUP BY customer_id  ORDER BY total_sales DESC LIMIT 20
                ),
                top20cc AS (
                    SELECT customer_id, top20cust.rank as customer_rank, category_id, top20cat.rank as category_rank FROM top20cat, top20cust
                )

                SELECT top20cc.*, COALESCE(SUM(sales_data.amount), 0) AS quantity_sold, COALESCE(SUM(sales_data.price*sales_data.amount), 0) AS dollar_value
                FROM top20cc
                    LEFT JOIN sales_data ON (
                        top20cc.customer_id = sales_data.customer_id AND top20cc.category_id = sales_data.category_id
                    )
                GROUP BY top20cc.customer_id, top20cc.customer_rank, top20cc.category_id, top20cc.category_rank
            """
        )



def build_options(settings):
    sc_options = SalesCubeOptions(settings)

    options_titles = {
        'total_sales': 'Show the total sales (total quantity sold and total dollar value) for each customer.(If customer C has made no purchases, still output C, with 0 quantity and dollars).',
        'total_sales_states': 'Show the total sales (total quantity sold and total dollar value) for each state.(If a state has 0 sales,  list it explicitly as such in the output).',
        'total_sales_product_customer': 'Show the total sales for each product, for a given customer. Only products that were actually bought by the given customer are listed. Order by dollar value. It is fine if your query hardcodes a specific customer id (full points).',
        'total_sales_all_products_all_customers': 'Show the total sales for each product and customer. Order by dollar value.Compared to 3. you will return all tuples 3. returns, plus also show entries for customers C and products P such that C did not buy P (list C, P with 0 total sales). ',
        'total_sales_product_category_state': 'Show the total sales for each product category and state.The output schema should be (category id, state).',
        'top20revenue': 'For each one of the top 20 product categories (by total revenue) and top 20 customers (by total purchase revenue), return a tuple (top product category ID, top customer ID, quantity sold, dollar value). It is possible that a top-20 customer spent $0 on a top-20 category. List this fact explicitly in the output: (id of “comic books”, id of “jane”, 0, 0) is possible. Extra credit if you can list the rank of the customer and of the category: (cat_id, cat_rank, cust_id, cust_rank, quantity, dollar value).'
    }

    options = [
        'total_sales',
        'total_sales_states',
        'total_sales_product_customer',
        'total_sales_all_products_all_customers',
        'total_sales_product_category_state',
        'top20revenue'
    ]

    for opt in options:
        sc_options.postgres_sql.queries.append(
            f'\n\n-- {options_titles[opt]}\n'
        )
        res = sc_options[opt]()
        sc_options.postgres_sql.queries.append(
           '\n'.join(['-- ' + v[0] for v in res[-2:]])
        )


    f_name = f'{settings["m3"]["file_suff"]}_sc.sql'
    with open(f'db/m3/files/{f_name}', 'w') as f:
        f.write('\n'.join(sc_options.postgres_sql.queries))

    print('Success')


if __name__ == '__main__':
    build_options(utils.get_settings())

