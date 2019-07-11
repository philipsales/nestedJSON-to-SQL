
# coding: utf-8

# In[699]:


import numpy as np
import pandas as pd
import json
from pandas.io.json import json_normalize 
from pandas import read_csv
import logging
from functools import reduce
import csv
from collections import defaultdict
import os


# In[700]:


## Configuration 


# In[728]:


#etl = 'kobo2elastic'
#etl = 'curis2elastic'
#etl = 'oldcuris2newcuris'
#etl = 'isabela2newaqm'
#etl = 'cambodia2newaqm'
#etl = 'cuartero2newaqm'
#etl = 'guimbal2newaqm'
etl = 'pototan2newaqm'

input_schema_file = ''
input_data_file = ''
mapping_file = ''

root_object = 'resident'

if etl == 'curis2elastic':
    #old curis to elasticsearch
    input_schema_file = 'schema/input/curisSchema.1-item.json'
    input_data_file = 'data/curisData.1-items.json'
    mapping_file = 'schema/map/couchbase2elastic.map.csv'
    output_dir = 'file/curisSchema/'
    
elif etl == 'kobo2elastic':
    #kobo to elasticsearch
    input_schema_file = 'schema/input/koboSchema.1-item.json'
    input_data_file = 'data/koboData.2-items.json'
    mapping_file = 'schema/map/kobo2elastic.map.csv'
    output_dir = 'file/koboSchema/'
    
elif etl == 'oldcuris2newcuris':
    #kobo to elasticsearch
    input_schema_file = 'schema/input/curisSchema.1-item.json'
    input_data_file = 'data/curisData.1-item.json'
    mapping_file = 'schema/map/kobo2elastic.map.csv'
    output_dir = 'file/curisSchema/'
    
elif etl == 'cambodia2newaqm':
    #kobo to elasticsearch
    input_schema_file = 'schema/input/KHMSchema.json'
    input_data_file = 'data/KHM.3-items.json'
    mapping_file = 'schema/map/kobo2elastic.map.csv'
    output_dir = 'file/KHMSchema/'

elif etl == 'cuartero2newaqm':
    input_schema_file = 'schema/input/curisSchema.1-item.json'
    input_data_file = 'data/Cuartero.RHU.15886.json'
    mapping_file = 'schema/map/aqmAllFields.csv'
    tmp_dir = 'data/processed/couchbase-curis-2019-06-21/tmp/'
    output_dir = 'data/processed/couchbase-curis-2019-06-21/'

elif etl == 'guimbal2newaqm':
    input_schema_file = 'schema/input/curisSchema.1-item.json'
    input_data_file = 'data/Guimbal.RHU.8154.json'
    mapping_file = 'schema/map/aqmAllFields.csv'
    tmp_dir = 'data/processed/couchbase-curis-2019-06-21/tmp/'
    output_dir = 'data/processed/couchbase-curis-2019-06-21/'

elif etl == 'pototan2newaqm':
    input_schema_file = 'schema/input/curisSchema.1-item.json'
    input_data_file = 'data/Pototan.RHU.6750.json'
    mapping_file = 'schema/map/aqmAllFields.csv'
    tmp_dir = 'data/processed/couchbase-curis-2019-06-21/tmp/'
    output_dir = 'data/processed/couchbase-curis-2019-06-21/'
    
elif etl == 'isabela2newaqm':
    #kobo to elasticsearch
    input_schema_file = 'schema/input/curisSchema.1-item.json'
    #input_data_file = 'data/Isabela.2-items.json'
    input_data_file = 'data/Isabela.RHU.6203-items.json'
    #mapping_file = 'schema/map/Isabela2newAQM.map.csv'
    #mapping_file = 'schema/map/aqmHealthInformationV1.csv'
    #mapping_file = 'schema/map/aqmGeneralQuestionsV1.csv'
    mapping_file = 'schema/map/aqmAllFields.csv'
    #mapping_file = 'schema/map/aqmRequiredFields.csv'
    #output_dir = 'file/IsabelaSchema/'
    tmp_dir = 'data/processed/couchbase-curis-2019-06-21/tmp/'
    output_dir = 'data/processed/couchbase-curis-2019-06-21/'


# In[702]:


def _open_file():
    with open(input_data_file) as f:
        return json.load(f)


# In[703]:


def _flatten_json(nested_json):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '.')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + '' + str(i) + '.')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(nested_json)
    return out


# In[704]:


def _dict_to_dataframe(dict_object):
    return pd.DataFrame.from_dict({'value': dict_object})


# In[705]:


## OPTIMIZE TO ACCEPT LIST NOT DATAFRAME
def filter_key(x):
    lists = format_key(x)
    lists = exclude_digit(lists)
    lists = list2string(lists)
    return lists

def filter_index_map(x):
    lists = format_key(x)
    lists = include_digit(lists)
    lists = list2string(lists)
    return lists

def format_key(items):
    #return list(map(lambda x:x.lower().split(sep='.'), items ))
    return items.lower().split(sep='.')

def include_digit(items):
    return [item for item in items if item.isdigit()]

def exclude_digit(items):
    return [item for item in items if not item.isdigit()]

def list2string(lists):
    return '.'.join(lists)


# In[706]:


def _add_custom_colums(dataframe_object):
    dataframe_object['key'] = list(dataframe_object.index)
    dataframe_object['key'] = dataframe_object['key'].apply(filter_key)
    #print('_add_custom_columns ',dataframe_object[dataframe_object['key'] == 'id' ])

    dataframe_object['_index_map'] = list(dataframe_object.index)
    dataframe_object['_index_map'] = dataframe_object['_index_map'].apply(filter_index_map)
    #TODO: musbe adjusted dynamically
    
    #CHECK if single object or array of objects
    #formid == koboschema
    #id == curischema
    #dataframe_object['_id'] = dataframe_object[dataframe_object['key'] == 'formid' ]['value'].values[0]
    
    #IF oldcursi2newcuris
    dataframe_object['_id'] = dataframe_object[dataframe_object['key'] == 'id' ]['value'].values[0]
    #IF KHMSChema
    #dataframe_object['_id'] = dataframe_object[dataframe_object['key'] == '_id' ]['value'].values[0]
    
    #print(dataframe_object)
    dataframe_object = dataframe_object.reset_index(drop=True)

    return dataframe_object


# In[707]:


## File loader


# In[708]:


def _get_mapping_fields():
    _mapping_df = pd.DataFrame()
    _mapping_df = read_csv(mapping_file).sort_values(['source_key'])

    _required_mapping_fields = list(_mapping_df['source_key'])
    return _required_mapping_fields


# In[709]:


def _get_index_map(data_df):
    return list(data_df['_index_map'].unique())


# In[710]:


def _filter_by_indexmap(data_df, index):
    return data_df[data_df['_index_map'] == index ]


# In[711]:


def clean_value(x):
    return x.lower().replace("/", ".").replace("the", "").replace("schema", "").strip()


# In[712]:


def _get_filenames():
    schema_csv = pd.read_csv(output_dir + 'schema.csv', skiprows=0)
    return schema_csv['file_name'].values[0].split(sep=",")


# In[713]:


def _get_csv_headers(filename):
    return pd.read_csv( output_dir + filename + '.csv',nrows=0) #get header only


# In[714]:


def _get_required_data(input_data_df,csv_header_df):
    
    required_header_list = [] 

    for data in input_data_df['key']:
        
        if data in csv_header_df.columns:
            
            required_header_list.append(data)
    
    #print(required_header_list)
    
    return input_data_df[input_data_df['key'].isin(required_header_list)]


# In[715]:


def _rename_index(data_df):
    if 'key' in data_df.columns:
        data_df.index = list(data_df['key'])
        data_df = data_df.drop('key', axis=1)
    return data_df 


# In[716]:


def _columnar_to_row(input_data_df,csv_header_df):
    
    for header in list(input_data_df.index):
        if header:
            csv_header_df.at['',header] = input_data_df.loc[header]['value']
    
        csv_header_df.at['','_id'] = input_data_df['_id'][header] 
        csv_header_df.at['','_index_map'] = input_data_df['_index_map'][header] 
         
    return csv_header_df


# In[717]:


def _write_to_csv(data_csv_df, filename):
    return data_csv_df.to_csv(output_dir + filename + '.csv', encoding='utf-8', mode='a', header=False,index=False)


# In[718]:


def write_processed_data_csv():
    #csv_header = {'value': [], 'key': [],  '_index_map':[],'_id': [] }
    empty_df = pd.DataFrame()
    empty_df.to_csv( tmp_dir + 'processed_data.csv', encoding='utf-8', mode='w',header=True,index=False)


# In[719]:


def write_required_data_csv():
    csv_header = {'value': [], 'key': [],  '_index_map':[],'_id': [] }
    empty_df = pd.DataFrame(csv_header)
    empty_df.to_csv( tmp_dir + 'required_data.csv', encoding='utf-8', mode='w',header=True,index=False)


# In[720]:


def get_required_data_only():
    data_list = _open_file()
    data_list
    
    _required_mapping_fields = []
    _required_mapping_fields = _get_mapping_fields()

    index = 0
    for datum in data_list:
        
        test_data_flat_dict = _flatten_json(datum)  
        
        test_data_flat_df = _dict_to_dataframe(test_data_flat_dict)

        _customed_data_flat_df = _add_custom_colums(test_data_flat_df)

        _filtered_customed_data_flat_df = _customed_data_flat_df[_customed_data_flat_df['key'].isin(_required_mapping_fields)]
        
        _filtered_customed_data_flat_df.to_csv(tmp_dir + 'required_data.csv', encoding='utf-8', mode='a', header=False,index=False)
        index += 1
        print('writing: ' + str(index) + '/' + str(len(data_list)) )


# In[721]:


def iterate_data():
    df = pd.read_csv(tmp_dir + 'required_data.csv', dtype={'_index_map': str})
    df = df.replace(np.nan,'',regex=True)
    df

    test_meta_header_df = pd.read_csv( 'data/processed/couchbase-curis-2019-06-21/_meta.csv')
    test_meta_header_df

    _filename_list = list(test_meta_header_df['file_name'].unique())
    _filename_list

    test_index_map =  list(df['_index_map'].unique())

    test_source_data_df = pd.DataFrame()

    print('Unique index: ',len(test_index_map))
    
    ##DELETE FILE IF EXIST
    os.remove(tmp_dir + 'processed_data.csv')
    
    for _test_index_map in test_index_map:
        print("------INDEX-MAP: ",str(_test_index_map))
        
        test_source_data_df = _filter_by_indexmap(df, str(_test_index_map))

        for _filename in _filename_list:
            test_list_field_name = []
            test_list_field_name = list(test_meta_header_df[test_meta_header_df['file_name'] == _filename ]['field_name']) 
            
            test_required_header_list = []
            test_required_header_list = list(set(test_source_data_df['key']) & set(test_list_field_name))
            
            req_test_source_data_df = pd.DataFrame()
            req_test_source_data_df = test_source_data_df[test_source_data_df['key'].isin(test_required_header_list)]

            test_rename_data_df = pd.DataFrame()
            test_rename_data_df = _rename_index(req_test_source_data_df)
            
            print('writing into filename:', _filename)
            
            test_rename_data_df.to_csv( tmp_dir + 'processed_data.csv', encoding='utf-8', mode='a',header=False,index=True)


# In[722]:


def read_processed_data():
    columns = ['key','value','_index_map','_id']
    
    testing_df = pd.read_csv(tmp_dir + 'processed_data.csv',header=None, names=columns, dtype={ '_index_map': str})
    testing_df = testing_df.replace(np.nan,'',regex=True)
    testing_df
    print('\n---Getting data from masterfile----')
    print('Total file rows', len(testing_df))
    print('Total resident:', len(testing_df['_id'].unique()))
    return testing_df


# In[723]:


def sort_fieldname_by_filename(testing_df):
    print('\n---grouping fields----')

    grouped = testing_df.groupby(['_index_map','_id'])
    
    print('Total group permutation:', len(grouped))
    index = 0

    for name, group in grouped:
        array_defaultdict = defaultdict(list)
        
        for r in zip(group['key'],group['value'],group['_index_map'],group['_id']):
            
            field_name = list(r)[0]
            
            test_meta_header_df = pd.read_csv('data/processed/couchbase-curis-2019-06-21/_meta.csv')    
            curr_filename = list(test_meta_header_df[test_meta_header_df['field_name'] == field_name  ]['file_name'])[0]

            array_tuples = []

            if array_defaultdict:
                if curr_filename in array_defaultdict:
                    array_defaultdict[curr_filename].append(r)  
                else:
                    array_defaultdict[curr_filename] = [r]
            else:
                array_defaultdict[curr_filename] = [r]
        
        row_data_csv_df = _new_columnar_to_row(array_defaultdict) 
        index += 1
        print('converting ' + str(index) + '/' + str(len(grouped)))


# In[724]:


def _new_columnar_to_row(defaultdict_test):
    
    for file ,dict_values in defaultdict_test.items():
        #print('inserting into: ', file)
        
        _filename = file
        test_meta_header_df = pd.read_csv( 'data/processed/couchbase-curis-2019-06-21/_meta.csv')    
    
        _id = ''
        _index_map = ''
        array_fields = []

        for tups in dict_values:
            fields = {}
            values = list(tups)

            _key = values[0]
            _value = values[1]
            _index_map = values[2]

            _id = values[3]

            fields = { _key : _value, '_index_map' : str(_index_map) + "", '_id': _id }
            array_fields.append(fields)

        new_list = {}
        
        if len(array_fields) > 1:
            for d in array_fields:
                new_list.update(d)
        else:
            new_list = array_fields[0]
            
        
        ## GET HEADER HERE
        csv_df = pd.read_csv(output_dir + file + '.csv', nrows=0 , dtype={'_index_map': str})

        ## MATCH THE CSV HEADER WITH  DATA
        for key,value in new_list.items():
            csv_df.at['',key] = value
        
        
        csv_df.to_csv(output_dir + file + '.csv', encoding='utf-8', mode='a', header=False,index=False)
         
    return


# In[725]:


write_required_data_csv()
write_processed_data_csv()

get_required_data_only()
iterate_data()

testing_df = read_processed_data()
sort_fieldname_by_filename(testing_df)


# ## MAPPING FIELDS

# In[ ]:


## =================================================


# In[ ]:


## Get Index_map unique values


# In[ ]:


## BUG: IF ADDRESS IS 
'''
    "answers": {
        "Address": [{
            "Location": {
                "Street": ["12312", "234234"],
                "City": "City"
            }
        }],
'''

