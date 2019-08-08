from pathlib import Path

import settings.couchbase_conf as couchbase_config
import settings.sqlite_conf as sqlite_conf

LOGGER = {
    'filenames': {
        'etl': 'etl',
        'kobo': 'kobo'
    }
}

DATA_TYPE = {
    'array': 'array',
    'string': 'string',
    'date': 'date',
    'integer': 'integer',
    'float': 'float',
    'object': 'object',
    'boolean': 'boolean'
}