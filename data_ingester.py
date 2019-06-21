
# coding: utf-8

# In[20]:


import numpy as np
import pandas as pd
import json
from pandas.io.json import json_normalize 
from pandas import read_csv
import logging
from functools import reduce
import csv


# In[21]:


## Configuration 


# In[22]:


#etl = 'kobo2elastic'
#etl = 'curis2elastic'
#etl = 'oldcuris2newcuris'
#etl = 'isabela2newaqm'
etl = 'cambodia2newaqm'

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
    input_data_file = 'data/KHM.3958-items.json'
    mapping_file = 'schema/map/kobo2elastic.map.csv'
    output_dir = 'file/KHMSchema/'

elif etl == 'isabela2newaqm':
    #kobo to elasticsearch
    input_schema_file = 'schema/input/curisSchema.1-item.json'
    input_data_file = 'data/Isabela.6203-items.json'
    output_dir = 'file/IsabelaSchema/'


# In[23]:


## Flatten json


# In[24]:


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


# In[25]:


## header filters


# In[26]:


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


# In[27]:


## File loader


# In[28]:


def _open_file():
    with open(input_data_file) as f:
        return json.load(f)
    
def _dict_to_dataframe(dict_object):
    return pd.DataFrame.from_dict({'value': dict_object})

def _add_custom_colums(dataframe_object):
    dataframe_object['key'] = list(dataframe_object.index)
    dataframe_object['key'] = dataframe_object['key'].apply(filter_key)
    print(dataframe_object[dataframe_object['key'] == 'id' ])

    dataframe_object['_index_map'] = list(dataframe_object.index)
    dataframe_object['_index_map'] = dataframe_object['_index_map'].apply(filter_index_map)
    #TODO: musbe adjusted dynamically
    
    #CHECK if single object or array of objects
    #formid == koboschema
    #id == curischema
    #dataframe_object['_id'] = dataframe_object[dataframe_object['key'] == 'formid' ]['value'].values[0]
    
    #IF oldcursi2newcuris
    #dataframe_object['_id'] = dataframe_object[dataframe_object['key'] == 'id' ]['value'].values[0]
    #IF KHMSChema
    dataframe_object['_id'] = dataframe_object[dataframe_object['key'] == '_id' ]['value'].values[0]
    
    #print(dataframe_object)
    dataframe_object = dataframe_object.reset_index(drop=True)

    return dataframe_object


# In[29]:


def _get_csv_headers(filename):
    return pd.read_csv( output_dir + filename + '.csv',nrows=0) #get header only


# In[30]:


def _get_required_data(input_data_df,csv_header_df):
    required_header_list = [] 

    for data in input_data_df['key']:
        if data in csv_header_df.columns:
            required_header_list.append(data)
    
    #print(required_header_list)
    return input_data_df[input_data_df['key'].isin(required_header_list)]

def _rename_index(data_df):
    if 'key' in data_df.columns:
        data_df.index = list(data_df['key'])
        data_df = data_df.drop('key', axis=1)
    return data_df 

def _columnar_to_row(input_data_df,csv_header_df):
    
    for header in list(input_data_df.index):
        if header:
            csv_header_df.at['',header] = input_data_df.loc[header]['value'] 
    

        csv_header_df.at['','_id'] = input_data_df['_id'][header] 
        csv_header_df.at['','_index_map'] = input_data_df['_index_map'][header] 
    return csv_header_df
    
def _write_to_csv(data_csv_df, filename):
    return data_csv_df.to_csv(output_dir + filename + '.csv', encoding='utf-8', mode='a', header=False,index=False)


# In[31]:


def _get_index_map(data_df):
    return list(data_df['_index_map'].unique())

def _get_filenames():
    schema_csv = pd.read_csv(output_dir + 'schema.csv', skiprows=0)
    return schema_csv['file_name'].values[0].split(sep=",")

def _filter_by_indexmap(data_df, index):
    return data_df[data_df['_index_map'] == index ]


# In[32]:


def clean_value(x):
    return x.lower().replace("/", ".").replace("the", "").replace("schema", "").strip()


# In[33]:


def _main(data_flat_df):
    index_map_list = []
    headers_list = []
    
    index_map_list = _get_index_map(data_flat_df)
    headers_list = _get_filenames()

    #TODO: remove filter by index
    #print(index_map_list)
    #print(headers_list)
    for index in index_map_list:
        for header in headers_list:
            #print(header)
            source_data_df = pd.DataFrame()
            source_data_df = _filter_by_indexmap(data_flat_df, index)
            source_data_df['key']  =  source_data_df['key'].apply(clean_value)

            csv_header_df = pd.DataFrame()
            csv_header_df = _get_csv_headers(header)
        
            new_data_df = pd.DataFrame()
            new_data_df = _get_required_data(source_data_df, csv_header_df)
            new_data_df = _rename_index(new_data_df)
            new_data_df = _columnar_to_row(new_data_df, csv_header_df)
            new_data_df = _write_to_csv(new_data_df, header)
    '''
    for header in headers_list:
        source_data_df = pd.DataFrame()
        source_data_df = _filter_by_indexmap(data_flat_df, '0')
        #source_data_df = data_flat_df
        source_data_df['key']  =  source_data_df['key'].apply(clean_value)
        #print(source_data_df)
        
        csv_header_df = pd.DataFrame()
        csv_header_df = _get_csv_headers(header)

        new_data_df = pd.DataFrame()
        new_data_df = _get_required_data(source_data_df, csv_header_df)
        new_data_df = _rename_index(new_data_df)
        new_data_df = _columnar_to_row(new_data_df, csv_header_df)
        new_data_df = _write_to_csv(new_data_df, header)
    '''
    return new_data_df


# In[34]:


def init(data_list):
    index = 0
    print('total item: ', len(data_list))
    for datum in data_list:
        index += 1
        print('index: ',index)

        data_flat_dict = {}
        data_flat_df = pd.DataFrame()
        
        data_flat_dict = _flatten_json(datum)
        
        data_flat_df = _dict_to_dataframe(data_flat_dict)
        data_flat_df = _add_custom_colums(data_flat_df)
        
        _main(data_flat_df)
    return


# In[35]:


data_list = []
data_list = _open_file()
#data_list
init(data_list)
#data_list


# In[36]:


## =================================================


# In[37]:


## Get Index_map unique values


# In[38]:


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

