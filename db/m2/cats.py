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

    def overall_likes(self):
        return self.postgres_sql.read_qry(
            f"""
                SELECT video_id, COUNT(*) as overall_likes
                FROM cats_201.likes
                GROUP BY video_id ORDER BY overall_likes DESC LIMIT 10
            """
        )

    def friends_likes(self):
        return self.postgres_sql.read_qry(
            f"""
                SELECT
                    video_id,
                    COUNT(*) as friends_likes
                FROM cats_201.likes WHERE
                user_id IN (
                    SELECT user_id_friend FROM cats_201.friends
                    WHERE user_id = 1
                ) OR user_id IN (
                    SELECT user_id FROM cats_201.friends
                    WHERE user_id_friend = 1
                )

                GROUP BY video_id ORDER BY friends_likes DESC LIMIT 10
            """
        )

    def friends_of_friends_likes(self):
        return self.postgres_sql.read_qry(
        f"""
            WITH uf AS (
                SELECT DISTINCT u1.user_id, u2.user_id as friend_id
                FROM cats_201.friends as u1
                INNER JOIN cats_201.friends as u2 ON u1.user_id = u2.user_id_friend
                UNION
                SELECT DISTINCT u1.user_id, u2.user_id as friend_id
                FROM cats_201.friends as u1
                INNER JOIN cats_201.friends as u2 ON u1.user_id_friend = u2.user_id
            ),
            uff AS (
                SELECT DISTINCT u1.user_id, u3.user_id as friend_id
                    FROM cats_201.friends as u1
                    INNER JOIN cats_201.friends as u2 ON u1.user_id = u2.user_id_friend
                    INNER JOIN cats_201.friends as u3 ON u2.user_id = u3.user_id_friend
                WHERE u1.user_id<>u3.user_id
                    UNION
                SELECT DISTINCT u1.user_id, u3.user_id as friend_id
                    FROM cats_201.friends as u1
                    INNER JOIN cats_201.friends as u2 ON u1.user_id_friend = u2.user_id
                    INNER JOIN cats_201.friends as u3 ON u2.user_id_friend = u3.user_id
                WHERE u1.user_id<>u3.user_id
            ),
            ufff AS (
                SELECT * FROM uf UNION SELECT * FROM uff
            ),
            ufff_videos AS (
            SELECT ufff.user_id, video_id, COUNT(*) AS amt_likes
            FROM cats_201.likes l, ufff
            WHERE l.user_id = ufff.friend_id
            GROUP BY ufff.user_id, video_id
            )
            SELECT * FROM ufff_videos ORDER BY amt_likes DESC LIMIT 10
        """
        )

    def my_kind_of_cats(self):
        return self.postgres_sql.read_qry(
        f"""
            WITH videos_liked_by_X AS (
                SELECT video_id FROM cats_201.likes WHERE user_id = 1
            ),
            users_liked_videos_liked_by_X AS (
                SELECT DISTINCT user_id FROM cats_201.likes WHERE video_id IN (
                    SELECT video_id FROM videos_liked_by_X
                )
            )

            SELECT video_id, COUNT(*) as amt_likes
            FROM cats_201.likes
            WHERE user_id IN (SELECT user_id FROM users_liked_videos_liked_by_X)
            GROUP BY video_id ORDER BY amt_likes DESC LIMIT 10
        """
        )

    def my_kind_of_cats_pref(self):
        return self.postgres_sql.read_qry(
        f"""
            WITH videos_liked_by_X AS (
                SELECT video_id FROM cats_201.likes WHERE user_id = 1
            ),
            users_liked_videos_liked_by_X AS (
                SELECT DISTINCT user_id FROM cats_201.likes WHERE video_id IN (
                    SELECT video_id FROM videos_liked_by_X
                )
            ),
            user_video_liked AS (
                SELECT user_id, video_id, COUNT(*) as amt_likes
                FROM cats_201.likes
                WHERE user_id IN (SELECT user_id FROM users_liked_videos_liked_by_X)
                GROUP BY user_id, video_id
            ),
            weighted_likes_by_user AS (
                SELECT user_id, LOG(1 + SUM(amt_likes)) AS weighted_likes
                FROM user_video_liked
                GROUP BY user_id
            )
            SELECT
                uvl.video_id,
                SUM(uvl.amt_likes * weighted_likes) AS weighted_likes_sum
                FROM user_video_liked as uvl
                INNER JOIN weighted_likes_by_user as wlu ON uvl.user_id=wlu.user_id
            GROUP BY uvl.video_id ORDER BY weighted_likes_sum DESC LIMIT 10
        """
        )



def build_options(settings):
    cat_options = CatsOptions(settings)

    options_titles = {
        'overall_likes': 'Option “Overall Likes”: The Top-10 cat videos are the ones that have collected the highest numbers of likes, overall.',
        'friends_likes': 'Option “Friend Likes”: The Top-10 cat videos are the ones that have collected the highest numbers of likes from the friends of X.',
        'friends_of_friends_likes': 'Option “Friends-of-Friends Likes”: The Top-10 cat videos are the ones that have collected the highest numbers of likes from friends and friends-of-friends.',
        'my_kind_of_cats': 'Option “My kind of cats”: The Top-10 cat videos are the ones that have collected the most likes from users who have liked at least one cat video that was liked by X.',
        'my_kind_of_cats_pref': 'Option “My kind of cats – with preference (to cat aficionados that have the same tastes)”: The Top-10 cat videos are the ones that have collected the highest sum of weighted likes from every other user Y (i.e., given a cat video, each like on it, is multiplied by a weight).',
    }

    options = [
        'overall_likes',
        'friends_likes',
        'friends_of_friends_likes',
        'my_kind_of_cats',
        'my_kind_of_cats_pref'
    ]

    for opt in options:
        cat_options.postgres_sql.queries.append(
            f'\n\n-- {options_titles[opt]}\n'
        )
        cat_options[opt]()


    with open(f'db/m2/files/Cats_M2_leonardo.sql', 'w') as f:
        f.write('\n'.join(cat_options.postgres_sql.queries))

    print('Success')


if __name__ == '__main__':
    build_options(utils.get_settings())

