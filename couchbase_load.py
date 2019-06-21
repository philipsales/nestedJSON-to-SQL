import json
import os 
import sys
import requests

import numpy as np
import pandas as pd
from pandas import read_csv
from pandas.io.json import json_normalize 

from couchbase.bucket import Bucket
from couchbase.n1ql import N1QLQuery, N1QLError
from couchbase.exceptions import CouchbaseTransientError
from couchbase.exceptions import CouchbaseNetworkError
from requests.exceptions import ConnectionError, RequestException 

import couchbase_conf

cb_conn = couchbase_conf.CouchbaseConfig[couchbase_conf.CouchbaseENV]

BUCKET = cb_conn['BUCKET'] 
URL = cb_conn['HOST'] + cb_conn['BUCKET']
IP_ADDRESS = cb_conn['IP'] 
TIMEOUTE = cb_conn['TIMEOUT']
PROTOCOL = cb_conn['PROTOCOL']
PORT = cb_conn['PORT']
API_ENDPOINT = "_bulk_docs"

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
            data=couchbase_json, 
            headers={"Accept":"application/json",
                "Content-type":"application/json"})
        print(r)         

        return r

    except (ConnectionError, RequestException, CouchbaseNetworkError) as err: 
        print(err) 
        sys.exit(1) 

def _push_doc_to_couchbase():
    headers = _conn_headers()
    filters = _conn_filters()
    url = _conn_url()
    data = [ {"foo": "bar"},{"bar": "foo"} ]
    print(data)

    _data_df = pd.DataFrame()

    #with open( 'file/parsed_output/Isabela/test.json') as f:
    with open( 'file/parsed_output/easycase.json') as f:
        _data_df = json.load(f)
        print(_data_df)



    try:


        for datum in _data_df:
            couchbase_json = datum.copy()
            cb_id = '2324234'
            rev_id = 'v1'
            couchbase_json["_rev"] = rev_id
            couchbase_json["_id"] = cb_id
            couchbase_json = json.dumps(datum)


            r = requests.post(url, 
                data=couchbase_json, 
                headers={"Accept":"application/json",
                        "Content-type":"application/json"})

            print(r.status_code)
            print(r.elapsed.total_seconds())
            print(r.text)


    except (ConnectionError, RequestException, CouchbaseNetworkError) as err: 
            print(err) 
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