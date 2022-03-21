import os
import sys

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
)

from src.db_mng import PostgresSql
from src.utils import utils


class CatsOptions():

    def __init__(self, settings):
        self.postgres_sql = PostgresSql(**settings['postgres'])

    def __getitem__(self, k):
        return getattr(self, k)

    def pcomp_table_user_video_liked(self):
        self.postgres_sql.perform_query('DROP TABLE IF EXISTS cats_201.user_video_liked')

        return self.postgres_sql.perform_query("""
            CREATE TABLE cats_201.user_video_liked AS (
                WITH videos_liked_by_X AS (
                    SELECT video_id FROM cats_201.likes WHERE user_id = 1
                ),
                users_liked_videos_liked_by_X AS (
                    SELECT DISTINCT user_id FROM cats_201.likes WHERE video_id IN (
                        SELECT video_id FROM videos_liked_by_X
                    )
                )
                SELECT user_id, video_id, COUNT(*) as amt_likes
                FROM cats_201.likes
                WHERE user_id IN (SELECT user_id FROM users_liked_videos_liked_by_X)
                GROUP BY user_id, video_id
            )

        """
        )

    def pcomp_table_weighted_likes_by_user(self):
        self.postgres_sql.perform_query('DROP TABLE IF EXISTS cats_201.weighted_likes_by_user')
        return self.postgres_sql.perform_query("""
            CREATE TABLE cats_201.weighted_likes_by_user AS (
                SELECT user_id, LOG(1 + SUM(amt_likes)) AS weighted_likes
                FROM cats_201.user_video_liked
                GROUP BY user_id
            )
        """)

    def index_user_video_liked(self):
        return self.postgres_sql.perform_query("""
            CREATE INDEX precomp_user_video_liked ON cats_201.user_video_liked(video_id, user_id);
        """)

    def index_weighted_likes_by_user(self):
        return self.postgres_sql.perform_query("""
            CREATE INDEX precomp_weighted_likes_by_user ON cats_201.weighted_likes_by_user(user_id);
        """)

    def my_kind_of_cats_pref(self):
        return self.postgres_sql.analyze_read_qry("""
            SELECT
                uvl.video_id,
                SUM(uvl.amt_likes * weighted_likes) AS weighted_likes_sum
                FROM cats_201.user_video_liked as uvl
                INNER JOIN cats_201.weighted_likes_by_user as wlu ON uvl.user_id=wlu.user_id
            GROUP BY uvl.video_id ORDER BY weighted_likes_sum DESC LIMIT 10
        """)


def build_options(settings):
    cat_options = CatsOptions(settings)



    options_titles = {
        'pcomp_table_user_video_liked': 'First Precomputed Table - table_user_video_liked',
        'pcomp_table_weighted_likes_by_user': 'Second Precomputed Table - weighted_likes_by_user',
        'index_user_video_liked': 'Index of First Precomputed Table - table_user_video_liked',
        'index_weighted_likes_by_user': 'Index of Second Precomputed Table - weighted_likes_by_user',
        'my_kind_of_cats_pref': 'New my_kind_of_cats_pref using precomputed-tables'
    }

    options = [
        'pcomp_table_user_video_liked',
        'pcomp_table_weighted_likes_by_user',
        'index_user_video_liked',
        'index_weighted_likes_by_user',
        'my_kind_of_cats_pref'
    ]


    for opt in options:
        cat_options.postgres_sql.queries.append(
            f'\n\n-- {options_titles[opt]}\n'
        )

        res = cat_options[opt]()
        if opt == 'my_kind_of_cats_pref':
            cat_options.postgres_sql.queries.append(
                '\n'.join(['-- ' + v[0] for v in res[-2:]])
            )

    with open(f'db/m4/files/cats.sql', 'w') as f:
        f.write('\n'.join(cat_options.postgres_sql.queries))

    print('Success')


if __name__ == '__main__':
    build_options(utils.get_settings())







