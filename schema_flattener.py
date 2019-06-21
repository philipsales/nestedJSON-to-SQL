
# coding: utf-8

# In[39]:


import logging
import json
import csv
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize 
from pandas import read_csv
from functools import reduce
from collections import defaultdict


# ## Configuration 

# In[40]:


#etl = 'kobo2elastic'
#etl = 'curis2elastic'
#etl = 'oldcuris2newcuris'
etl = 'isabela2newaqm'

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


# ## Get input JSON Schema (draft 07)

# In[41]:


def _get_schema():
    _data_df = pd.DataFrame()
    
    with open(input_schema_file) as f:
        _data_df = json_normalize(json.load(f))
    
    return _data_df


# In[42]:


def _get_json_schema_properties(schema_df):
    _required_properties = []
    _required_fields = pd.DataFrame()

    for prop in list(schema_df):
        if "._" not in prop:
            if len(prop.split(sep='.')) > 2:
                if prop.split(sep='.')[-1] == 'type' or prop.split(sep='.')[-1] == 'title':
                    _required_properties.append(prop)
    
    _required_fields = schema_df[_required_properties]
    
    return _required_fields


# In[43]:


def _get_avro_schema_properties(schema_df):
    pass


# In[44]:


def _clean_value(value):
    return value.lower().replace("/", ".").replace("the", "").replace("schema", "").strip()

def _clean_index(index):
    return index.lower().replace("/", ".").replace("properties.", "").replace("items.", "").strip()


# In[45]:


def _clean_schema(required_fields_df):
    _newSchema_df = pd.DataFrame()
    _required_fields_df = required_fields_df

    _newSchema_df['value'] = _required_fields_df.T[0].apply(_clean_value)
    _newSchema_df.reset_index(level=0, inplace=True)
    _newSchema_df['index'] = _newSchema_df['index'].apply(_clean_index)
    _newSchema_df.sort_values(['index'])
    
    return _newSchema_df


# In[46]:


def _get_key_type(newSchema_df):
    _valueSchema_df = pd.DataFrame()
    _newSchema_df = newSchema_df

    schema_length = len(_newSchema_df)
    title_counter = 0
    type_counter = 1
    skip = 2

    title_property = []
    type_property = []

    while (title_counter < schema_length):
        title_property.append(_newSchema_df.iloc[title_counter]['index'].replace('.title',''))
        title_counter += skip

    while (type_counter < schema_length):
        type_property.append(_newSchema_df.iloc[type_counter]['value'])
        type_counter += skip

    _valueSchema_df['source_key'] = title_property
    _valueSchema_df['source_type'] = type_property

    _valueSchema_df = _valueSchema_df.sort_values(['source_key']).reset_index(drop=True)
    _valueSchema_df.loc[_valueSchema_df['source_type'] == 'array']

    return _valueSchema_df


# ## Create root or default file (e.g. main, resident)

# In[47]:


def _get_type_array(kv_schema_df):
    _type_array_df = pd.DataFrame()
    _type_array_list = []
    
    _type_array_df = kv_schema_df.loc[kv_schema_df['source_type'] == 'array']
    _type_array_df = _type_array_df.reset_index(drop=True)

    _type_array_list = list(_type_array_df['source_key']) #array type
    
    return _type_array_list


# ## TODO: Include all primitives

# In[48]:


def _get_type_primitive(kv_schema_df):
    _type_primitive_df = pd.DataFrame()
    _type_primitive_df = kv_schema_df.loc[kv_schema_df['source_type'] == 'string']

    _type_primitive_list = []
    _type_primitive_list = list(_type_primitive_df['source_key']) 
    return _type_primitive_list


# In[49]:


def _get_type_list(property_type_primitives, property_type_array):
    list_fields = []

    for primitive_field in property_type_primitives:
        for array_field in property_type_array:

            tmp_array = primitive_field.split(sep='.')

            if(len(tmp_array)) > 1:
                if primitive_field == array_field: #for array list
                    list_fields.append(primitive_field)
            elif(len(tmp_array)) == 1:
                if primitive_field == array_field:
                    list_fields.append(primitive_field)

    return list_fields


# In[50]:


def _get_type_primitive_root(property_type_primitives, property_type_array):
    root_primitive_fields = []

    for primitive_field in property_type_primitives:
        for array_field in property_type_array:

            tmp_array = primitive_field.split(sep='.')
        
            if(len(tmp_array)) == 1:
                
                if primitive_field != array_field:
                    root_primitive_fields.append(primitive_field)

    root_primitive_fields = list(dict.fromkeys(root_primitive_fields))
    root_primitive_fields = list(set(root_primitive_fields) - set(property_type_array))
    
    return root_primitive_fields


# In[51]:


def _segregate_fields(property_type_primitives, property_type_array):
    property_types_dd = defaultdict(list)

    primitive_fields = []
    list_fields = []

    for primitive_field in property_type_primitives:
        for array_field in property_type_array:

            tmp_array = primitive_field.split(sep='.')

            if(len(tmp_array)) > 1:

                if primitive_field == array_field: #for array list
                    list_fields.append(primitive_field)

            elif(len(tmp_array)) == 1:

                if primitive_field == array_field:
                    list_fields.append(primitive_field)
                else:
                    primitive_fields.append(primitive_field)

    property_types_dd['root_primitive']  = list(dict.fromkeys(primitive_fields))
    property_types_dd['list']  = list_fields
    #print(list(property_types_dd['root_primitive']))
    return property_types_dd


# In[52]:


def _write_main_file():
    with open(output_dir + root_object + '.csv', 'w'):
        pass


# In[53]:


def _write_non_main_file(array_type_field):

    for field_name in list(array_type_field):
        with open(output_dir + field_name + '.csv', 'w'):
            pass
        
    return


# In[54]:


def _write_type_list_header(lists):

    for list_field in lists:
        _columns_list = []
        _columns_list.append('_id')
        _columns_list.append('_index_map')
        _columns_list.append(list_field)
        
        list_type_fields  = pd.DataFrame(columns = _columns_list)
        list_type_fields.to_csv(output_dir + list_field + '.csv', encoding='utf-8', mode='a', index=False)
    return 


# ## Create headers for type: array of objects and primitive type (int, str) @object level

# In[55]:


def _get_type_object(property_type_array, property_type_primitives, property_type_primitive_root):
    _array_objects = set()
    _array_objects = set(property_type_primitives) - set(property_type_array) - set(property_type_primitive_root)
    _array_objects_list = list(_array_objects)

    object_type_dd = defaultdict(list)
    non_root_header_object = []
    root_header_object = []

    for name in _array_objects_list:
        str1 = name.split('.')
        str2 = '.'.join(str1[0:-1])
        

        if str2 in property_type_array:
            non_root_header_object.append(name)
        else:
            root_header_object.append(name)

    object_type_dd['non_root_objects']  = non_root_header_object
    object_type_dd['root_objects']  = root_header_object
    
    #print(object_type_dd['non_root_objects'])
    
    _dd_objects = defaultdict(list)
    _dd_non_root = defaultdict(list)
    _dd_root = defaultdict(list)
    
    for header in object_type_dd['non_root_objects']:
        
        filenames = header.split(sep=".")[0:-1]
        filenames = '.'.join(filenames)
        
        _dd_non_root[filenames].append(header)
    
    for header in object_type_dd['root_objects']:
        filenames = header.split(sep=".")[0:1]
        filenames = '.'.join(filenames)

        if filenames in property_type_array:
            #print(filenames)
            _dd_non_root[filenames].append(header)
        else:
            _dd_root[root_object].append(header)
    
    _dd_objects['_dd_non_root_objects'] = _dd_non_root
    _dd_objects['_dd_root_objects'] = _dd_root
    
    #print(list(_dd_objects))
    return _dd_objects


# In[56]:


'''

def _get_dd_objects(object_type_dd):
    
    _dd_objects = defaultdict(list)
    _dd_non_root = defaultdict(list)
    _dd_root = defaultdict(list)
    
    for header in object_type_dd['non_root_header_object']:
        filenames = header.split(sep=".")[0:-1]
        filenames = '.'.join(filenames)
        _dd_non_root[filenames].append(header)
    
    for header in object_type_dd['root_header_object']:
        filenames = header.split(sep=".")[0:1]
        filenames = '.'.join(filenames)

        if filenames in property_type_array:
            #print(filenames)
            _dd_non_root[filenames].append(header)
        else:
            _dd_root[root_object].append(header)
    
    _dd_objects['_dd_non_root'] = _dd_non_root
    _dd_objects['_dd_root'] = _dd_root
    
    return _dd_objects
'''


# ## Write headers for type: array of object

# In[57]:


def _write_type_array_object_header(dd_non_root):
    for header in dd_non_root:
        _columns_list = []
        _columns_list.append('_id')
        _columns_list.append('_index_map')
        _columns_list += list(dd_non_root[header])

        object_array_csv_df  = pd.DataFrame(columns = _columns_list)
        object_array_csv_df.to_csv(output_dir + header + '.csv', encoding='utf-8', mode='a', index=False)


# In[58]:


def _write_type_main_header(root_primitive, root_object):
    
    _main_header_list = []
    _main_header_list.append('_id')
    _main_header_list.append('_index_map')
    
    #include root primitives
    _main_header_list += root_primitive
    #include root objects
    _main_header_list += root_object
    
    
    print(_main_header_list)
    main_resident_df  = pd.DataFrame(columns = _main_header_list)
    main_resident_df.to_csv(output_dir + 'resident' + '.csv', encoding='utf-8', mode='a', index=False)
    main_resident_df


# In[59]:


def _write_schema_definition(property_type_array):
    
    filenames_list = property_type_array 
  
    filenames_list.append('resident')

    filenames_str = ",".join(filenames_list)

    schema_desc_df = pd.DataFrame() 
    schema_desc_df['file_name'] = [filenames_str]
    schema_desc_df['file_count']  = len(filenames_list)
    schema_desc_df['date'] = pd.to_datetime('today')
    schema_desc_df['source_schema'] = input_schema_file 
    schema_desc_df['version'] = '1.0'
    schema_desc_df.to_csv(output_dir + 'schema.csv', encoding='utf-8', mode='w', index=False)
    return schema_desc_df


# In[60]:


schema_df = _get_schema()


# In[61]:


required_fields_df = _get_json_schema_properties(schema_df)


# In[62]:


clean_schema_df = _clean_schema(required_fields_df)


# In[63]:


kv_schema_df = _get_key_type(clean_schema_df)


# In[64]:


property_type_array = _get_type_array(kv_schema_df)


# In[65]:


property_type_primitives = _get_type_primitive(kv_schema_df)


# In[66]:


property_type_list = _get_type_list(property_type_primitives, property_type_array)


# In[67]:


property_type_primitive_root = _get_type_primitive_root(property_type_primitives, property_type_array)


# In[68]:


#property_type_dd = _segregate_fields(property_type_primitives ,property_type_array)


# In[69]:


_write_main_file()


# In[70]:


_write_non_main_file(property_type_array)


# In[71]:


_write_type_list_header(property_type_list)


# In[72]:


type_object_dd = _get_type_object(property_type_array, property_type_primitives, property_type_primitive_root)


# In[73]:


_write_type_array_object_header(type_object_dd['_dd_non_root_objects'])


# In[74]:


root_primitive = []
root_object = []
root_primitive = property_type_primitive_root
root_object = type_object_dd['_dd_root_objects']['resident']
_write_type_main_header(root_primitive, root_object)


# In[75]:


_write_schema_definition(property_type_array)


# ### BUGS LIST: 
# ### 1. unique identifier must be defined
# ### 2. fields starting with underscore are discareded
