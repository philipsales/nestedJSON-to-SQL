import json
import os 
import sys
import requests

from couchbase.bucket import Bucket
from couchbase.n1ql import N1QLQuery, N1QLError
from couchbase.exceptions import CouchbaseTransientError
from couchbase.exceptions import CouchbaseNetworkError
from requests.exceptions import ConnectionError, RequestException 

from settings.base_conf import couchbase_config, sqlite_conf

from pipeline.extract import sqlite

from pipeline.auxiliary import sqlite_checker

import lib.logs.logging_conf, logging
logger = logging.getLogger("couchbase.syncgateway")

cb_conn = couchbase_config.CouchbaseConfig[couchbase_config.CouchbaseENV]
sqlite_conn = sqlite_conf.SQLiteConfig[sqlite_conf.SQLiteENV]

BUCKET = cb_conn['BUCKET'] 
URL = cb_conn['HOST'] + cb_conn['BUCKET']
IP_ADDRESS = cb_conn['IP'] 
TIMEOUTE = cb_conn['TIMEOUT']
PROTOCOL = cb_conn['PROTOCOL']
PORT = cb_conn['PORT']
API_ENDPOINT = "_bulk_docs"

SQLITE_PATH = sqlite_conn['PATH']

def init_couchbase():
    headers = _conn_headers()
    filters = _conn_filters()
    url = _conn_url()

    try:
        r = requests.get(url, headers = headers, params = filters)
        logger.info(r.status_code)
        logger.info(r.elapsed.total_seconds())
        logger.info(r.json())
        return _dict2json(r.json()["results"])

    except (ConnectionError, RequestException, CouchbaseNetworkError) as err: 
        logger.error(err) 
        sys.exit(1)

def push_couchbase(data):
    url_bulk_docs = _conn_url(api_endpoint="_bulk_docs")
    conn = sqlite.create_connection(SQLITE_PATH)

    if(data["new_data"] != []):
        _bulk_push_to_couchbase(conn, url_bulk_docs, data['new_data'])

    if(data["old_data"] != []):
        _push_doc_to_couchbase(conn, data["old_data"])

    sqlite.close_db(conn)

def _bulk_push_to_couchbase(conn, url, data):
    try:

        couchbase_json = {
            "docs": data,
            "new_edits": True
        }
        
        couchbase_json = json.dumps(couchbase_json)

        r = requests.post(url, 
            data=couchbase_json, 
            headers={"Accept":"application/json",
                "Content-type":"application/json"})
                
        logger.info(r.status_code)
        logger.info(r.elapsed.total_seconds())
        logger.info(r.text)

        sqlite_checker.update_many_kobo(conn, data, r.json())

        return r

    except (ConnectionError, RequestException, CouchbaseNetworkError) as err: 
        logger.error(err) 
        sys.exit(1) 

def _push_doc_to_couchbase(conn, data):

    try:

        for datum in data:
        
            (kobo_id, cb_id, rev_id) = sqlite_checker.get_id(conn, datum["kobo_id"])
            url_update_doc = _conn_url(api_endpoint=cb_id)

            couchbase_json = datum.copy()
            couchbase_json["_rev"] = rev_id
            couchbase_json["_id"] = cb_id

            couchbase_json = json.dumps(couchbase_json)

            r = requests.put(url_update_doc, 
                data=couchbase_json, 
                headers={"Accept":"application/json",
                        "Content-type":"application/json"})

            logger.info(r.status_code)
            logger.info(r.elapsed.total_seconds())
            logger.info(r.text)

            sqlite_checker.update_one_kobo(conn, couchbase_json, r.json())

    except (ConnectionError, RequestException, CouchbaseNetworkError) as err: 
            logger.error(err) 
            sys.exit(1) 

def _conn_headers():
    #TODO make dynamic pass kwargs
    return {
        "accept": "application/json",
        "allow_redirects": "True",
        "timeout": str(TIMEOUTE),
    }

def _conn_filters(**kwargs):
    #TODO make dynamic pass kwargs
    return  {
        "access" : "false",
        "channels": "false",
        "include_docs": "true",
        "revs": "false",
        "update_seq": "false",
        "limit":"5",
        "since":"200"
    }

def _conn_url(**kwargs):
    protocol = PROTOCOL
    ip_address = IP_ADDRESS
    port = PORT 
    bucket = BUCKET
    api_endpoint = kwargs.get('api_endpoint', "")

    urls = protocol + "://"  + ip_address + ":" + port + "/"  + bucket + "/" + api_endpoint  
    logger.info(urls)
    return urls

if __name__ == "__main__":
    init_couchbase()

def _dict2json(results):
    counter = 0
    data = []

    for row in results: 
        doc = row["doc"]
        doc["cb_id"] = doc.pop('_id')
        data.append(json.dumps(doc))
        counter += 1
        logger.info(counter)

    return data