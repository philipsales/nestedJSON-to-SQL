
#SERVER Configuration
CouchbaseENV = "local"
#CouchbaseENV = "testing-miko-linode"
#CouchbaseENV = "prod-clone"
#CouchbaseENV = "latest-uat"

#9100 = port for direct N1QL
#8091 = admin console 

CouchbaseConfig = {
    'local': {
        'BUCKET': 'awhdispergodb',
        'USERNAME': 'adminadmin',
        'PASSWORD': 'adminadmin',
        'PROTOCOL': 'http',
        'SCHEME': 'couchbase',
        'IP': '127.0.0.1',
        'HOST': 'couchbase://127.0.0.1/',
        'PORT': '8091',
        'TIMEOUT': 7200
    },
    'development': {
        'BUCKET': 'awhcurisdb010832',
        'USERNAME': '',
        'PASSWORD': '',
        'PROTOCOL': 'http',
        'SCHEME': 'couchbase',
        'IP': '172.104.49.91',
        'HOST': 'couchbase://172.104.49.91/',
        'PORT': '4984',
        'TIMEOUT': 7200
    },
    'uat': {
        'BUCKET': 'awhcurisdb',
        'USERNAME': '',
        'PASSWORD': '',
        'PROTOCOL': 'http',
        'SCHEME': 'couchbase',
        'IP': '139.162.18.54',
        'HOST': 'couchbase://139.162.18.54/',
        'PORT': '4984',
        'TIMEOUT': 7200
    },
    'production': {
        'BUCKET': 'awhcurisdb',
        'USERNAME': 'superman',
        'PASSWORD': 'kryptonite',
        'PROTOCOL': 'http',
        'SCHEME': 'couchbase',
        'IP': '13.76.6.56',
        'HOST': 'couchbase://13.76.6.56/',
        'PORT': '4984',
        'TIMEOUT': 7200
    },
    'testing-miko-linode': {
        'BUCKET': 'awhcurisdb',
        'USERNAME': 'superman',
        'PASSWORD': 'kryptonite',
        'PROTOCOL': 'http',
        'SCHEME': 'couchbase',
        'IP': '13.76.152.243',
        'HOST': 'couchbase://13.76.152.243/',
        'PORT': '4984',
        'TIMEOUT': 7200
    },
    'prod-clone': {
        'BUCKET': 'awhcurisdb',
        'USERNAME': 'superman',
        'PASSWORD': 'kryptonite',
        'PROTOCOL': 'http',
        'SCHEME': 'couchbase',
        'IP': '13.76.45.110',
        'HOST': 'couchbase://13.76.45.110/',
        'PORT': '4984',
        'TIMEOUT': 7200
    },
    'latest-uat': {
        'BUCKET': 'awhcurisdb',
        'USERNAME': 'superman',
        'PASSWORD': 'kryptonite',
        'PROTOCOL': 'http',
        'SCHEME': 'couchbase',
        'IP': '139.162.49.49',
        'HOST': 'couchbase://139.162.49.49/',
        'PORT': '4984',
        'TIMEOUT': 7200
    },
}

