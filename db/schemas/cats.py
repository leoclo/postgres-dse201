from collections import OrderedDict

CATS_USERS = 'users'
CATS_VIDEOS = 'videos'
CATS_LIKES = 'likes'
CATS_WATCH =  'watch'
CATS_FRIENDS = 'friends'
CATS_LOGS = 'logs'
CATS_SUGGESTIONS = 'suggestions'

FOR_ON = 'ON DELETE CASCADE ON UPDATE CASCADE'

CATS_SCHEMA = {
    'name': 'cats_201',
    'tables': OrderedDict({
        CATS_USERS: {
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
                    'name': 'username',
                    'type': 'TEXT UNIQUE'
                },
                {
                    'name': 'fb_login',
                    'type': 'TEXT UNIQUE'
                }
            ]
        },
        CATS_VIDEOS: {
            'columns': [
                {
                    'name': 'id',
                    'type': 'SERIAL PRIMARY KEY'
                },
                {
                    'name': 'title',
                    'type': 'TEXT UNIQUE NOT NULL'
                },
                {
                    'name': 'description',
                    'type': 'TEXT NULL'
                }
            ]
        },
        CATS_LIKES: {
            'columns': [
                {
                    'name': 'user_id',
                    'type': 'INT NOT NULL'
                },
                {
                    'name': 'video_id',
                    'type': 'INT NOT NULL'
                },
                {
                    'name': 'when_time',
                    'type': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL'
                }
            ],
            'constraints': [
                {
                    'name': f'fk_{CATS_LIKES}_{CATS_USERS}',
                    'type': 'FOREIGN KEY(user_id)',
                    'spec': f'{CATS_USERS}(id) {FOR_ON}'
                },
                {
                    'name': f'fk_{CATS_LIKES}_{CATS_VIDEOS}',
                    'type': 'FOREIGN KEY(video_id)',
                    'spec': f'{CATS_VIDEOS}(id) {FOR_ON}'
                }
            ],
            'index': {
                'i_type': 'UNIQUE INDEX',
                'name': f'{CATS_LIKES}_unique_index',
                'cols': ['user_id', 'video_id']
            }
        },
        CATS_WATCH: {
            'columns': [
                {
                    'name': 'user_id',
                    'type': 'INT'
                },
                {
                    'name': 'video_id',
                    'type': 'INT'
                },
                {
                    'name': 'when_time',
                    'type': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL NOT NULL'
                }
            ],
            'constraints': [
                {
                    'name': f'fk_{CATS_WATCH}_{CATS_USERS}',
                    'type': 'FOREIGN KEY(user_id)',
                    'spec': f'{CATS_USERS}(id) {FOR_ON}'
                },
                {
                    'name': f'fk_{CATS_WATCH}_{CATS_VIDEOS}',
                    'type': 'FOREIGN KEY(video_id)',
                    'spec': f'{CATS_VIDEOS}(id) {FOR_ON}'
                }
            ]
        },
        CATS_LOGS: {
            'columns': [
                {
                    'name': 'id',
                    'type': 'SERIAL PRIMARY KEY'
                },
                {
                    'name': 'user_id',
                    'type': 'INT'
                },
                {
                    'name': 'logged_in',
                    'type': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL'
                }
            ],
            'constraints': [
                {
                    'name': f'fk_{CATS_LOGS}_{CATS_USERS}',
                    'type': 'FOREIGN KEY(user_id)',
                    'spec': f'{CATS_USERS}(id) {FOR_ON}'
                }
            ]
        },
        CATS_SUGGESTIONS: {
            'columns': [
                {
                    'name': 'log_id',
                    'type': 'INT'
                },
                {
                    'name': 'video_id',
                    'type': 'INT'
                }
            ],
            'constraints': [
                {
                    'name': f'fk_{CATS_SUGGESTIONS}_{CATS_VIDEOS}',
                    'type': 'FOREIGN KEY(video_id)',
                    'spec': f'{CATS_VIDEOS}(id) {FOR_ON}'
                },
                {
                    'name': f'fk_{CATS_SUGGESTIONS}_{CATS_LOGS}',
                    'type': 'FOREIGN KEY(log_id)',
                    'spec': f'{CATS_LOGS}(id) {FOR_ON}'
                }
            ]
        },
        CATS_FRIENDS: {
            'columns': [
                {
                    'name': 'user_id',
                    'type': 'INT'
                },
                {
                    'name': 'user_id_friend',
                    'type': 'INT'
                }
            ],
            'constraints': [
                {
                    'name': f'fk_{CATS_FRIENDS}_{CATS_USERS}',
                    'type': 'FOREIGN KEY(user_id)',
                    'spec': f'{CATS_USERS}(id) {FOR_ON}',
                },
                {
                    'name': f'fk_{CATS_FRIENDS}_{CATS_USERS}_2',
                    'type': 'FOREIGN KEY(user_id_friend)',
                    'spec': f'{CATS_USERS}(id) {FOR_ON}'
                }
            ],
            'index': {
                'i_type': 'UNIQUE INDEX',
                'name': f'{CATS_FRIENDS}_unique_index',
                'cols': ['user_id', 'user_id_friend']
            }
        }
    })
}



CATS_CSV_DATA = {
    'name': CATS_SCHEMA['name'],
    'tables': [
        {
            'table_name': CATS_USERS,
            'file_name': 'Cats_users',
            'rename_cols': {
                'uid': 'id',
                'fname': 'first_name',
                'lname': 'last_name'
            }
        },
        {
            'table_name': CATS_VIDEOS,
            'file_name': 'Cats_videos',
            'rename_cols': {
                'vid': 'id',
                'vname': 'title'
            }
        },
        {
            'table_name': CATS_LIKES,
            'file_name': 'Cats_likes',
            'rename_cols': {
                'uid': 'user_id',
                'vid': 'video_id'
            }
        },
        {
            'table_name': CATS_FRIENDS,
            'file_name': 'Cats_friends',
            'rename_cols': {
                'uid': 'user_id',
                'ufid': 'user_id_friend'
            }
        }
    ]
}

