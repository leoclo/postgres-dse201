import itertools
import math
import pandas as pd
import random
import os

STATES = [
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID',
    'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS',
    'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK',
    'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV',
    'WI', 'WY'
]

STATE_NAMES = [
    'Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado',
    'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho',
    'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine',
    'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi',
    'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey',
    'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio',
    'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina',
    'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia',
    'Washington', 'West Virginia', 'Wisconsin', 'Wyoming'
]

class AssembleDf():

    def __init__(self, amount_rows, dir_path):
        self.amount_rows = amount_rows

        self.fn = pd.read_csv(
            f'{dir_path}/txt_files/first_names.txt', header=None, names=['fn']
        )
        self.ln = pd.read_csv(
            f'{dir_path}/txt_files/last_names.txt', header=None, names=['ln']
        )

    def __getitem__(self, k):
        return getattr(self, k)

    def cats_users(self):
        lns = 1
        fns = self.amount_rows
        if self.amount_rows >= 10000:
            lns = math.floor(self.amount_rows/10000)
            fns = 10000

        return pd.DataFrame(
            itertools.product(
                self.fn['fn'].values[0:fns], self.ln['ln'].values[0:lns]
            ), columns=['fname', 'lname']
        ).reset_index().rename(columns={"index": "uid"})

    def cats_videos(self):
        return pd.DataFrame(
            [[f'video_{i}'] for i in range(self.amount_rows)],
            columns=['vname']
        ).reset_index().rename(columns={"index": "vid"})

    def cats_likes(self):
        return pd.DataFrame(
            [
                [i, random.randint(0, self.amount_rows-2)]
                for i in range(self.amount_rows)
                for j in range(random.randint(0, 5))
            ],
            columns=['uid', 'vid']
        ).drop_duplicates()

    def cats_friends(self):
        return pd.DataFrame(
            [
                [i, random.randint(0, self.amount_rows-2)]
                for i in range(self.amount_rows)
                for j in range(random.randint(0, 5))
            ],
            columns=['uid', 'ufid']
        ).drop_duplicates()

    def sales_cube_states(self):
        return pd.DataFrame(
            [[n, s] for n, s in zip(STATE_NAMES, STATES)],
            columns=['stname', 'stabbrev']
        ).reset_index().rename(columns={"index": "sid"})

    def sales_cube_customers(self):
        lns = 1
        fns = self.amount_rows
        if self.amount_rows >= 10000:
            lns = math.floor(self.amount_rows/10000)
            fns = 10000
        data = itertools.product(
            self.fn['fn'].values[0:fns], self.ln['ln'].values[0:lns]
        )
        return pd.DataFrame(
            [[d[0], d[1]] + [random.randint(0, 49)] for d in data],
            columns=['fname', 'lname', 'state']

        ).reset_index().rename(columns={"index": "id"})

    def sales_cube_categories(self):
        desc = 'Products in this category have properties PSET'
        return pd.DataFrame(
            [[f'C{i}', f'{desc}{i}' ] for i in range(10)],
            columns=['cname', 'cdescr']
        ).reset_index().rename(columns={"index": "id"})

    def sales_cube_products(self):
        return pd.DataFrame([
                [f'P{i}', random.uniform(80, 999), random.randint(0, 9)
            ] for i in range(60)],
            columns=['pname', 'price' , 'cid']
        ).reset_index().rename(columns={"index": "id"})

    def sales_cube_products(self):
        return pd.DataFrame([
                [f'P{i}', random.uniform(80, 999), random.randint(0, 9)
            ] for i in range(60)],
            columns=['pname', 'price' , 'cid']
        ).reset_index().rename(columns={"index": "id"})

    def sales_cube_sales(self):
        return pd.DataFrame([
                [
                    random.randint(0, 59),
                    i,
                    random.randint(1, 59),
                    round(random.random(), 2)
                ]
                for i in range(self.amount_rows)
                for j in range(random.randint(0, 2))
            ],
            columns=['pid', 'cid', 'quantity', 'discount']
        ).reset_index().rename(columns={"index": "id"})


def build_csvs(amount_rows):
    dfs = [
        # 'cats_users',
        # 'cats_videos',
        # 'cats_likes',
        # 'cats_friends',
        'sales_cube_states',
        'sales_cube_customers',
        'sales_cube_categories',
        'sales_cube_products',
        'sales_cube_sales'
    ]

    dir_path = os.path.dirname(__file__)
    a_df = AssembleDf(amount_rows, dir_path)

    for name in dfs:
        df = a_df[name]()
        df.to_csv(f'{dir_path}/{name.capitalize()}.csv', index=False)

    print('Files generated sucessfully')


if __name__ == '__main__':
    amount_rows = 200000
    build_csvs(amount_rows)