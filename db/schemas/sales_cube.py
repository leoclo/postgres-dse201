from collections import OrderedDict

SALES_STATES = 'states'
SALES_CUSTOMERS = 'customers'
SALES_CATEGORIES = 'categories'
SALES_PRODUCTS = 'products'
SALES_SALES = 'sales'

SALES_SCHEMA = {
    'name': 'sales_cube',
    'tables': OrderedDict({
        SALES_STATES: {
            'columns': [
                {
                    'name': 'state',
                    'type': 'CHAR(2) PRIMARY KEY'
                },
                {
                    'name': 'name',
                    'type': 'TEXT NOT NULL'
                }
            ]
        },
        SALES_CUSTOMERS: {
            'columns': [
                {
                    'name': 'id',
                    'type': 'SERIAL PRIMARY KEY'
                },
                {
                    'name': 'first_name',
                    'type': 'TEXT NOT NULL'
                },
                {
                    'name': 'last_name',
                    'type': 'TEXT NOT NULL'
                },
                {
                    'name': 'state',
                    'type': 'CHAR(2) NOT NULL'
                }
            ],
            'constraints': [
                {
                    'name': f'fk_{SALES_CUSTOMERS}_{SALES_STATES}',
                    'type': 'FOREIGN KEY(state)',
                    'spec': f'{SALES_STATES}(state) ON DELETE CASCADE'
                }
            ]
        },
        SALES_CATEGORIES: {
            'columns': [
                {
                    'name': 'id',
                    'type': 'SERIAL PRIMARY KEY'
                },
                {
                    'name': 'name',
                    'type': 'TEXT NOT NULL'
                },
                {
                    'name': 'description',
                    'type': 'TEXT NOT NULL'
                }
            ]
        },
        SALES_PRODUCTS: {
            'columns': [
                {
                    'name': 'id',
                    'type': 'SERIAL PRIMARY KEY'
                },
                {
                    'name': 'name',
                    'type': 'TEXT NOT NULL'
                },
                {
                    'name': 'category_id',
                    'type': 'INT NOT NULL'
                },
                {
                    'name': 'list_price_cents',
                    'type': 'INT NOT NULL'
                }
            ],
            'constraints': [
                {
                    'name': f'fk_{SALES_PRODUCTS}_{SALES_CATEGORIES}',
                    'type': 'FOREIGN KEY(category_id)',
                    'spec': f'{SALES_CATEGORIES}(id) ON DELETE CASCADE'
                }
            ]
        },
        SALES_SALES: {
            'columns': [
                {
                    'name': 'id',
                    'type': 'SERIAL PRIMARY KEY'
                },
                {
                    'name': 'product_id',
                    'type': 'INT NOT NULL'
                },
                {
                    'name': 'customer_id',
                    'type': 'INT NOT NULL'
                },
                {
                    'name': 'amount',
                    'type': 'NUMERIC NOT NULL'
                },
                {
                    'name': 'discount',
                    'type': 'DECIMAL'
                }
            ],
            'constraints': [
                {
                    'name': f'fk_{SALES_SALES}_{SALES_PRODUCTS}',
                    'type': 'FOREIGN KEY(product_id)',
                    'spec': f'{SALES_PRODUCTS}(id) ON DELETE CASCADE'
                },
                {
                    'name': f'fk_{SALES_SALES}_{SALES_CUSTOMERS}',
                    'type': 'FOREIGN KEY(customer_id)',
                    'spec': f'{SALES_CUSTOMERS}(id) ON DELETE CASCADE'
                }
            ]
        }
    })
}