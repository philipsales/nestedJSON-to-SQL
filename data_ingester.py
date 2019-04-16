
# coding: utf-8

# In[47]:


import numpy as np
import pandas as pd
import json
from pandas.io.json import json_normalize 
from pandas import read_csv
import logging
from functools import reduce
import csv


# ## Configuration 

# In[48]:


etl = 'curis2elastic'

input_schema_file = ''
input_data_file = ''
mapping_file = ''
    
if etl == 'curis2elastic':
    #old curis to elasticsearch
    input_schema_file = 'schema/input/curisSchema.1-item.json'
    input_data_file = 'data/curisData.2-actual-items.json'
    mapping_file = 'schema/map/couchbase2elastic.map.csv'
elif etl == 'kobo2elastic':
    #kobo to elasticsearch
    input_schema_file = 'schema/input/aqmSchema.complete.json'
    input_data_file = 'data/aqmData.2-items.json'
    mapping_file = 'schema/map/kobo2elastic.map.csv'
elif etl == 'oldcuris2newcuris':
    #kobo to elasticsearch
    input_schema_file = 'schema/input/curisData.1-Schema.avro.json'
    input_data_file = 'data/curisData.1-items.json'
    mapping_file = 'schema/map/kobo2elastic.map.csv'


# ## Flatten json

# In[49]:


def _flatten_json(nested_json):
    """
        Flatten json object with nested keys into a single level.
        Args:
            nested_json: A nested json object.
        Returns:
            The flattened json object if successful, None otherwise.
    """
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


# ## header filters

# In[50]:


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


# ## File loader

# In[51]:


def open_file():
    with open(input_data_file) as f:
        return json.load(f)


# ## Load Data

# In[52]:


data_list = []
data_list = open_file()


# ## TODO: Iterate here if multiple arrays

# In[53]:


data_flat_dict = {}
data_flat_dict = _flatten_json(data_list[0])


# ## Convert dictionary to dataframe

# In[54]:


def _dict_to_dataframe(dict_object):
    return pd.DataFrame.from_dict({'value': dict_object})


# In[55]:


data_flat_df = pd.DataFrame()
data_flat_df = _dict_to_dataframe(data_flat_dict)


# ## Add columns: _index_map and _id

# In[56]:


def _add_custom_colums(dataframe_object):
    dataframe_object['key'] = list(dataframe_object.index)
    dataframe_object['key'] = dataframe_object['key'].apply(filter_key)

    dataframe_object['_index_map'] = list(dataframe_object.index)
    dataframe_object['_index_map'] = dataframe_object['_index_map'].apply(filter_index_map)

    dataframe_object['_id'] = dataframe_object[dataframe_object['key'] == 'awh_id']['value'].values[0]

    dataframe_object = dataframe_object.reset_index(drop=True)

    return dataframe_object


# In[57]:


root_data_df = pd.DataFrame()
root_data_df = _add_custom_colums(data_flat_df)
root_data_df


# ## Filter input data by index_map

# In[58]:


root_data_df = data_flat_df[data_flat_df['_index_map'] == '' ]


# ## Get CSV headers for main @root

# In[59]:


def _get_csv_headers(filename):
    file_dir = 'file/'
    return pd.read_csv( file_dir + filename + '.csv',nrows=0) #get header only


# In[60]:


root_csv_df = pd.DataFrame()
root_csv_df = _get_csv_headers('resident')
root_csv_df


# ## Get input data matching the CSV headers for main@root

# ## Get input data with headers for main @root

# In[61]:


def _get_required_data(input_data_df,csv_header_df):
    required_header_list = [] 

    for data in input_data_df['key']:
        if data in csv_header_df.columns:
            required_header_list.append(data)
             
    return input_data_df[input_data_df['key'].isin(required_header_list)]

def _rename_index(data_df):
    if 'key' in data_df.columns:
        data_df.index = list(data_df['key'])
        data_df = data_df.drop('key', axis=1)
    return data_df 

def _columnar_to_row(input_data_df,csv_header_df):
    
    for header in list(input_data_df.index):
        csv_header_df.at['',header] = input_data_df.loc[header]['value'] 
        
    csv_header_df.at['','_id'] = input_data_df['_id'][header] 
    csv_header_df.at['','_index_map'] = input_data_df['_index_map'][header] 
    return csv_header_df
    
def _write_to_csv(data_csv_df, filename='resident'):
    file_dir = 'file/'
    return data_csv_df.to_csv( file_dir + filename + '.csv', encoding='utf-8', mode='a', header=False,index=False)


# In[62]:



new_data_df = pd.DataFrame()
new_data_df = _get_required_data(root_data_df, root_csv_df)
new_data_df


# In[63]:


new_data_df = _rename_index(new_data_df)
new_data_df


# In[64]:


new_data_df = _columnar_to_row(new_data_df, root_csv_df)
new_data_df


# In[65]:


new_data_df = _write_to_csv(new_data_df, 'resident')
new_data_df


# ## =================================================

# ## Get Index_map unique values

# In[69]:


index_map_list = [] 
index_map_list = list(data_flat_df['_index_map'].unique())

#excluse root
index_map_list[1:]


# ## Get filenames

# In[72]:


schema_csv = pd.read_csv('file/schema.csv', skiprows=0)
filenames_list = list(schema_csv['file_name'])
filenames_list

