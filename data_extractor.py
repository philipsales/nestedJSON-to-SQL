
import datetime as dt
import shutil

from requests.exceptions import ConnectionError, RequestException

def _get_data_stream():
    
    db_source = 'couchbase'
    system = 'curis'
    date =  dt.datetime.utcnow()
    fileName = db_source + '-' + system + '-' + str(date)
    fileType = 'json'
    fileDir = 'data/extracted/'

    local_filename = fileDir + fileName + '.' + fileType 

    r = [{"foo": "bar"}]

    #TODO: error if file already exist. won't overwrite
    with open(local_filename, 'ab') as f:
        shutil.copyfileobj(r.raw, f)

if __name__ == '__main__':
    _get_data_stream()
