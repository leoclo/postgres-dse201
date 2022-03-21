

-- Fisrt Precomputed Table - table_user_video_liked

DROP TABLE IF EXISTS cats_201.user_video_liked;
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
            );


-- Second Precomputed Table - weighted_likes_by_user

DROP TABLE IF EXISTS cats_201.weighted_likes_by_user;
CREATE TABLE cats_201.weighted_likes_by_user AS (
                SELECT user_id, LOG(1 + SUM(amt_likes)) AS weighted_likes
                FROM cats_201.user_video_liked
                GROUP BY user_id
            );


-- Index of Fisrt Precomputed Table - table_user_video_liked

CREATE INDEX precomp_user_video_liked ON cats_201.user_video_liked(video_id, user_id);;


-- Index of Second Precomputed Table - weighted_likes_by_user

CREATE INDEX precomp_weighted_likes_by_user ON cats_201.weighted_likes_by_user(user_id);;


-- New my_kind_of_cats_pref using precomputed-tables

EXPLAIN ANALYZE SELECT
                uvl.video_id,
                SUM(uvl.amt_likes * weighted_likes) AS weighted_likes_sum
                FROM cats_201.user_video_liked as uvl
                INNER JOIN cats_201.weighted_likes_by_user as wlu ON uvl.user_id=wlu.user_id
            GROUP BY uvl.video_id ORDER BY weighted_likes_sum DESC LIMIT 10;
-- Planning Time: 0.303 ms
-- Execution Time: 3.345 ms