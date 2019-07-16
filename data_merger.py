
# coding: utf-8

# In[187]:


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
import uuid


# In[219]:


etl = 'cuartero2newaqm'

input_schema_file = ''
input_data_file = ''
mapping_file = ''

datelog_dir = 'couchbase-curis-2019-06-21-cuartero'

if etl == 'cuartero2newaqm':
    schema_meta_file = '_meta.csv'
    schema_meta_dir = 'data/processed/' + datelog_dir + '/'
    schema_meta_path = schema_meta_dir + schema_meta_file
    
    #mapping_file = '2.1.TestAQMHealthInfoQuestions.V1.map.csv'
    #mapping_file = '2.1.AQMGeneralQuestions.V1.map.csv'
    mapping_file = '2.2.AQMHealthInfoQuestions.V1.map.csv'
    #mapping_file = '2.6.AQMMentallHealthQuestions.V1.map.csv'
    #mapping_file = '2.7.AWHDisabilityQuestions.V1.map.csv'
    #mapping_file = '1.1.AQMPersonalQuestions.V1.map.csv'
    
    mapping_dir = 'schema/map/Philippines/' 
    mapping_path = mapping_dir + mapping_file
    
    #TODO: add cleaned_dir
    tmp_dir = 'data/processed/' + datelog_dir + '/tmp/'
    processed_dir = 'data/cleaned/' + datelog_dir + '/'
    merged_dir = 'data/merged/' + datelog_dir + '/'
    
    


# ## TODO: datecreated date format must be UTF8

# ## TODO: DISABILTY CAUSE MUST BE ARRAY NOT STRING!!

# ## TODO: CREATE FOLDER DIRECTORY MKDIR
# ## TODO: PARSED FIELD TYPE: LIST -- merged into single object

# ## TODO: FIX ID MERGING

# ## TODO: FIX _id into profileId

# # READ MAPPING FILE

# In[189]:


#def _get_mapping_fields():
_mapping_df = pd.DataFrame()
_mapping_df = read_csv(mapping_path).sort_values(['source_key']).replace(np.nan,'',regex=True)
_mapping_df.head(3)


# # GET THE FIELDS IN THE MAPPING FILE

# In[190]:


mapping_fields_list = []
mapping_fields_list = list(filter(None, (_mapping_df['source_key'].unique())))
mapping_fields_list


# # GET THE FIELDS IN THE SCHEMA META (i.e. _meta) FILE

# In[191]:


meta_headers_df = pd.DataFrame()
meta_headers_df = pd.read_csv(schema_meta_path)
meta_headers_df.head(5)


# ## MATCH THE FIELDS IN MAPPING FILE AND SCHEMA META FILE

# In[192]:


match_headers_df = pd.DataFrame()
match_headers_df = meta_headers_df[meta_headers_df['field_name'].isin(mapping_fields_list)]
match_headers_df = match_headers_df.sort_values(['file_name','field_name']).reset_index(drop=True)
match_headers_df


# # CREATE DEFAULT DICT FOR FILENAME AS KEY and FIELD NAMES AS VALUE

# In[193]:


filename_per_field_dd = defaultdict(list)

for index,row in match_headers_df.iterrows():
    filename = row['file_name']
    fields = row['field_name']
    filename_per_field_dd[filename].append(fields)  
    
filename_per_field_dd


# ## CREATE OUTPUT FILE WITH DYNAMIC NAME DERIVED FROM MAPPING FILE

# In[194]:


_output_filename = mapping_file.split(sep='.')[2]
_output_filename


# ## REMOVE OUTPUT FILE IF EXISTING

# In[195]:


if os.path.exists(merged_dir + _output_filename + '.csv'):
    os.remove(merged_dir + _output_filename + '.csv' )


# ## WRITE EMPTY CSV FOR MERGE.csv

# ### TODO: FILENAME MUST BE DYNAMIC

# ## HARDCODE TEST DATA

# In[196]:


test_filename_per_field_dd = {
    'health_informations': ['health_informations.allergies',
              'health_informations.blood_pressure.first_reading.diastole',
              'health_informations.blood_pressure.first_reading.systole',
              'health_informations.blood_pressure.second_reading.diastole',
              'health_informations.blood_pressure.second_reading.systole',
              'health_informations.blood_sign',
              'health_informations.blood_type',
              'health_informations.exercise_in_a_week',
              'health_informations.smoking_habit'],
    'health_informations.family_history': ['health_informations.family_history']}

test_filename_per_field_dd = {'profiles': ['profiles.civil_status','profiles.employment.is_employed','profiles.education','profiles.employment.nature','profiles.religion']}
test_filename_per_field_dd = {
        'health_informations': [
                  'health_informations.allergies',
                  'health_informations.blood_pressure.first_reading.diastole',
                  'health_informations.blood_pressure.first_reading.systole',
                  'health_informations.blood_sign',
                  'health_informations.exercise_in_a_week',
                  'health_informations.smoking_habit'],
        'health_informations.family_history': [
                  'health_informations.family_history'],
        'profiles': ['profiles.civil_status'],
        'resident': ['gender','registered_at','user-cam.id']}


# ## DYNAMIC TEST DATA

# In[197]:


test_filename_per_field_dd = filename_per_field_dd


# ## CREATE OUTPUT FILE WITH HEADERS BASED ON _META AND MAPPING FILE

# In[198]:


fields_list = list(test_filename_per_field_dd.values())
flat_fields_list = [item for sublist in fields_list for item in sublist]
all_fields_list  = ['_id','_index_map'] + flat_fields_list

empty_data_df = pd.DataFrame(columns=all_fields_list)
empty_data_df.to_csv(merged_dir + _output_filename + '.csv', encoding='utf-8', mode='w', header=True,index=False)
empty_data_df


# ## TODO: DYNAMICALLY MERGE DATA FROM DIFFERENT 

# In[199]:


fields_list = list(test_filename_per_field_dd.values())
flat_fields_list = [item for sublist in fields_list for item in sublist]
flat_fields_list


# In[200]:


list(test_filename_per_field_dd.items())


# ## TODO: identify here if primitive or list. if list .agg to_list

# In[201]:


match_headers_df


# In[202]:


filename
match_headers_df[match_headers_df['file_name'] == filename][['field_type','file_name']]


# In[203]:


list(match_headers_df[match_headers_df['file_name'] == filename][['field_type','file_name']]['field_type'].unique())


# ## TODO: optimize here. tmp_df will run out of memory

# ## TODO: TODO: if index_map = 0, replace with empty

# In[204]:


def _array_to_list(_test_df):
    return _test_df.groupby('_id').agg(lambda x: x.tolist())


# In[205]:


def _flatten_index_map(index):
    _new_index = ''
    
    if index == '0':
        _new_index = '' 
    elif isinstance(index,list):
        _new_index = ''
    else:
        _new_index = index
        
    return _new_index


# In[206]:


_tmp_df = pd.DataFrame(columns=['_id','_index_map'])
_tmp_df.to_csv(merged_dir + _output_filename + '.csv',index=False)

for filename, fields in test_filename_per_field_dd.items():
    
    _mandatory_fields = ['_id','_index_map']
    _fields = list(fields + _mandatory_fields)
    
    _field_type = list(match_headers_df[match_headers_df['file_name'] == filename][['field_type','file_name']]['field_type'].unique())[0]
    
    
    _test_df = pd.DataFrame()
    _test_df = pd.read_csv(processed_dir + str(filename) + '.csv', dtype={"_index_map": str}).sort_values(['_id','_index_map']) 
    
    _test_df = _test_df[_fields].replace(np.nan,'',regex=True).reset_index(drop=True)
    
    if _field_type == 'list':
        _test_df = _array_to_list(_test_df)

    _test_df['_index_map'] = _test_df['_index_map'].apply(_flatten_index_map)
    
    print('Start Merging ', filename)
    _tmp_df = _tmp_df.merge(_test_df,on=["_id","_index_map"], how="outer",  suffixes=('_x', '_y') ).replace(np.nan,'',regex=True)
    

_tmp_df.to_csv(merged_dir + _output_filename + '.csv',index=False)
_tmp_df.T


# ## WRITE MERGE DATA TO CSV

# In[207]:


test_group_df = pd.read_csv(merged_dir + _output_filename + '.csv',dtype={'_index_map': str}).replace(np.nan,'',regex=True)


# In[208]:


test_group_df.T


# ## RENAME THE HEADERS USING MAPPED HEADERS

# In[209]:


source_destination_keys_df = pd.DataFrame()
source_destination_keys_df = _mapping_df[['source_key','destination_key']]


# In[210]:


new_column_name_dict = dict(zip(source_destination_keys_df['source_key'], source_destination_keys_df['destination_key']))
new_column_name_dict 


# ## GET THE DATA from merged dataframes

# In[211]:


_required_data_df = pd.DataFrame()
_required_data_df = test_group_df


# ## APPLICABLE only if not Resident data

# ## DO NECESSARY TRANSFORMATION

# In[212]:


_required_data_df.rename(columns = new_column_name_dict,inplace=True )

## APPLICABLE only if NOT Resident data
_required_data_df.rename(columns={'_id': 'profileId'}, inplace=True)
_required_data_df.head(3)


# ## GENERATE _id column

# In[213]:


_required_data_df['_id'] = _required_data_df.index.to_series().map(lambda x: uuid.uuid4())
_required_data_df['id'] = _required_data_df['_id']
_required_data_df


# ## CLEAR _INDEX_MAP since each _id has generaed ID

# In[214]:


_required_data_df['_index_map'] = ''
_required_data_df


# ## GET THE NEW FIELDS WITH THE DEFAULT VALUES

# In[215]:


new_fields_df = _mapping_df[_mapping_df['data_type'] == 'new'][['destination_key', 'default_value']]
new_fields_df


# ## APPEND THE NEW FIELDS AS COLUMN

# In[216]:


for index,row in new_fields_df.iterrows():
    _header = row['destination_key']
    _value = row['default_value']
    
    _required_data_df[_header] = _value

_required_data_df = _required_data_df.reset_index(drop=True)
_required_data_df.head(5)


# ## SORT COLUMN HEADERS

# In[217]:


_sorted_columns = sorted(list(_required_data_df.columns))
_required_data_df = _required_data_df[_sorted_columns]
_required_data_df


# # WRITE TO CSV FINALIZED
# 

# In[218]:


_required_data_df.to_csv(merged_dir + _output_filename + '.csv', encoding='utf-8', mode='w', header=True,index=False)

