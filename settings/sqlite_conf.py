
#SERVER Configuration
SQLiteENV = "local"

SQLiteConfig = {
    'development': {
        'PATH': './logs/sqlite/kobosqlite_dev.db',
        'DB_NAME': 'kobosqlite_local',
        'USERNAME': '',
        'PASSWORD': '',
        'PROTOCOL': '',
        'SCHEME': 'sqlite',
        'IP': '',
        'HOST': '',
        'PORT': '',
        'VERSION':'v1',
        'TIMEOUT': 7200
    },
    'local': {
        'PATH': './logs/sqlite/kobosqlite_local.db',
        'DB_NAME': 'kobosqlite_dev',
        'USERNAME': '',
        'PASSWORD': '',
        'PROTOCOL': '',
        'SCHEME': 'sqlite',
        'IP': '',
        'HOST': '',
        'PORT': '',
        'VERSION':'v1',
        'TIMEOUT': 7200
    },
    'uat': {
        'PATH': './data/kobosqlite_uat.db',
        'DB_NAME': 'kobosqlite',
        'USERNAME': '',
        'PASSWORD': '',
        'PROTOCOL': '',
        'SCHEME': 'sqlite',
        'IP': '',
        'HOST': '',
        'PORT': '',
        'VERSION':'v1',
        'TIMEOUT': 7200
    },
    'production': {
        'PATH': './data/kobosqlite_prod.db',
        'DB_NAME': 'kobosqlite',
        'USERNAME': '',
        'PASSWORD': '',
        'PROTOCOL': '',
        'SCHEME': 'sqlite',
        'IP': '',
        'HOST': '',
        'PORT': '',
        'VERSION':'v1',
        'TIMEOUT': 7200
    }
}