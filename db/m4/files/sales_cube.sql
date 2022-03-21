

-- First Precomputed Table - sales_cube.sales_data

DROP TABLE IF EXISTS sales_cube.sales_data;
CREATE TABLE sales_cube.sales_data AS (
                SELECT s.customer_id, s.product_id, s.amount, pr.price, pr.category_id, cust.state_id
                FROM sales_cube.sales s
                INNER JOIN sales_cube.products pr ON (s.product_id = pr.id)
                INNER JOIN sales_cube.customers cust ON (s.customer_id = cust.id)
            );


-- Second Precomputed Table - top20cc

DROP TABLE IF EXISTS sales_cube.top20cc;
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
        );


-- Index of First Precomputed Table - sales_cube.sales_data

CREATE INDEX index_precomp_sales_data ON sales_cube.sales_data(customer_id);;


-- Index of Second Precomputed Table - sales_cube.top20cc

CREATE INDEX index_precomp_top20cc ON sales_cube.top20cc(category_id);;


-- New top20revenue using precomputed tables

EXPLAIN ANALYZE SELECT sales_cube.top20cc.*, COALESCE(SUM(sales_cube.sales_data.amount), 0) AS quantity_sold, COALESCE(SUM(sales_cube.sales_data.price*sales_cube.sales_data.amount), 0) AS dollar_value
            FROM sales_cube.top20cc
                LEFT JOIN sales_cube.sales_data ON (
                    sales_cube.top20cc.customer_id = sales_cube.sales_data.customer_id AND sales_cube.top20cc.category_id = sales_cube.sales_data.category_id
                )
            GROUP BY sales_cube.top20cc.customer_id, sales_cube.top20cc.customer_rank, sales_cube.top20cc.category_id, sales_cube.top20cc.category_rank;
-- Planning Time: 0.268 ms
-- Execution Time: 18.720 ms