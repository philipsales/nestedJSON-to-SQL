import json
import os 
import sys
import requests
import sqlite3
import datetime as dt

import numpy as np
import pandas as pd
from pandas import read_csv
from pandas.io.json import json_normalize 

from couchbase.admin import Admin
from couchbase.bucket import Bucket
from couchbase.n1ql import N1QLQuery, N1QLError
from couchbase.exceptions import CouchbaseTransientError
from couchbase.exceptions import CouchbaseNetworkError
from requests.exceptions import ConnectionError, RequestException 

from pipeline.extract import sqlite


import couchbase_conf
from settings.base_conf import sqlite_conf

cb_conn = couchbase_conf.CouchbaseConfig[couchbase_conf.CouchbaseENV]
sqlite_conn = sqlite_conf.SQLiteConfig[sqlite_conf.SQLiteENV]

BUCKET = cb_conn['BUCKET'] 
BUCKET_USERNAME = cb_conn['USERNAME'] 
BUCKET_PASSWORD = cb_conn['PASSWORD'] 
URL = cb_conn['HOST'] + cb_conn['BUCKET']
IP_ADDRESS = cb_conn['IP'] 
TIMEOUT = cb_conn['TIMEOUT']
PROTOCOL = cb_conn['PROTOCOL']
PORT = cb_conn['PORT']
API_ENDPOINT = "_bulk_docs"

SQLITE_PATH = sqlite_conn['PATH']

'''
TODO: set UTC as ISO 8601
TODO: Isolate SQLITE functions
'''

'''
def init_couchbase():
    headers = _conn_headers()
    filters = _conn_filters()
    url = _conn_url()

    try:
        r = requests.get(url, headers = headers, params = filters)
        return _dict2json(r.json()["results"])

    except (ConnectionError, RequestException, CouchbaseNetworkError) as err: 
        print(err) 
        sys.exit(1)
'''

def _bulk_push_to_couchbase():
    headers = _conn_headers()
    print(headers)
    filters = _conn_filters()
    url = _conn_url()

    data = [ {"foo": "bar"},{"bar": "foo"} ]
    print(url)
    print(data)

    try:
        
        couchbase_json = json.dumps(data)
        print(couchbase_json)

        r = requests.post(url, 
            auth=('adminadmin','adminadmin'),
            data=couchbase_json, 
            headers={"Accept":"application/json",
                "Content-type":"application/json"})
        print(r)         

        return r

    except (ConnectionError, RequestException, CouchbaseNetworkError) as err: 
        print(err) 
        sys.exit(1) 

def create_table(conn):
    cursor = conn.cursor()
    try:
        query = ("CREATE TABLE IF NOT EXISTS "
            + " etl_insert_seq " 
            + "(id INTEGER PRIMARY KEY, " 
            + " last_id TEXT, "
            + " date TEXT,  "
            + " notes TEXT)")

        cursor.execute(query)
        conn.commit()
    except Exception as e:
        # Roll back any change if something goes wrong
        conn.rollback()
        raise e

def last_seq_insert(last_count_seq):
    with open( 'file/parsed_output/Sample/output.json') as f:
        datum  = json.load(f)

    conn = sqlite.create_connection(SQLITE_PATH)
    _last_seq = last_count_seq

    cursor = conn.cursor()

    try:
        query = ("INSERT INTO " 
            + " etl_insert_seq " 
            + "(last_id, date, notes) "
            + "VALUES(?,?, ?)")

        values = (_last_seq, dt.datetime.utcnow(), "Notes")
        result = cursor.execute(query, values)

        print("User inserted")
        conn.commit()
    except sqlite3.IntegrityError:
        print('Record already exists')

def _push_doc_to_couchbase():
    headers = _conn_headers()
    filters = _conn_filters()
    url = _conn_url()

    _data_df = pd.DataFrame()

    print(headers)

    try:
        count = 0
        with open( 'file/parsed_output/Guimbal/output.json') as f:
        #with open('data/merged/couchbase-curis-2019-06-21-cuartero/health_information.output.json') as f:
        #with open('data/merged/couchbase-curis-2019-06-21-cuartero/AQMGeneralQuestions.output.json') as f:
            for datum in json.load(f):

                couchbase_json = json.dumps(datum)

                print(url)
                r = requests.post(url,
                    data=couchbase_json, 
                    headers=headers,
                    auth=(BUCKET_USERNAME, BUCKET_PASSWORD))
                    #auth=('curisWebAppUser', 'adm(1)mwh'))


                count += 1
                print('------Count: '+ str(count))
                print('status: '+ str(r.status_code))
                print('elapsed_time: '+str(r.elapsed.total_seconds()))
                print('response: ' + r.text)

    ##TODO: connection error timeout for POST
    except (ConnectionError, RequestException, CouchbaseNetworkError) as err: 
            last_count_seq(count)
            print(err) 
            #sys.exit(1) 

def _conn_headers():
    #TODO make dynamic pass kwargs
    return {
        "Accept": "application/json",
        "Content-type": "application/json",
        "timeout": str(TIMEOUT),
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
    print(urls)
    return urls


def _dict2json(results):
    counter = 0
    data = []

    for row in results: 
        doc = row["doc"]
        doc["cb_id"] = doc.pop('_id')
        data.append(json.dumps(doc))
        counter += 1
        print(counter)

    return data

if __name__ == "__main__":
    #init_couchbase()
    _push_doc_to_couchbase()
    #_bulk_push_to_couchbase()