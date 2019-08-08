
# coding: utf-8

# In[623]:


import os
import json
import csv
import logging
import itertools
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize 
from pandas import read_csv
from functools import reduce
from collections import defaultdict


# In[636]:


etl = 'test2newaqm'

if etl == 'pototan2newaqm':
    resident_file = 'file/parsed_output/Pototan/resident.csv'
    parsed_output_csv_file = 'file/parsed_output/Pototan/output.csv'
    parsed_output_json_file = 'file/parsed_output/Pototan/output.json'
    
elif etl == 'cambodia2newaqm':
    #kobo to elasticsearch
    resident_file = 'file/parsed_output/Cambodia/resident.csv'
    parsed_output_csv_file = 'file/parsed_output/Cambodia/output.csv'
    parsed_output_json_file = 'file/parsed_output/Cambodia/output.json'
    
elif etl == 'isabela2newaqm':
    #kobo to elasticsearch
    resident_file = 'file/parsed_output/Isabela/resident.csv'
    parsed_output_csv_file = 'file/parsed_output/Isabela/output.csv'
    parsed_output_json_file = 'file/parsed_output/Isabela/output.json'
    
elif etl == 'guimbal2newaqm':
    #kobo to elasticsearch
    resident_file = 'file/parsed_output/Guimbal/resident.csv'
    parsed_output_csv_file = 'file/parsed_output/Guimbal/output.csv'
    parsed_output_json_file = 'file/parsed_output/Guimbal/output.json'
    
elif etl == 'pototan2newaqm':
    #kobo to elasticsearch
    resident_file = 'file/parsed_output/Pototan/resident.raw.csv'
    parsed_output_csv_file = 'file/parsed_output/Pototan/output.csv'
    parsed_output_json_file = 'file/parsed_output/Pototan/output.json'

elif etl == 'cuartero2newaqm':
    #kobo to elasticsearch
    resident_file = 'file/parsed_output/Cuartero/resident.csv'
    parsed_output_csv_file = 'file/parsed_output/Cuartero/output.csv'
    parsed_output_json_file = 'file/parsed_output/Cuartero/output.json'

elif etl == 'test2newaqm':
    datelog_dir = 'couchbase-curis-2019-06-21-pototan'
    
    ##TODO: Add cleaned_dir!!!
    #processed_dir    = 'data/processed/' + datelog_dir + '/'
    processed_dir    = 'data/cleaned/' + datelog_dir + '/'
    tmp_dir    = 'data/processed/' + datelog_dir + '/tmp/'
    output_dir = 'data/processed/' + datelog_dir + '/'
    merged_dir = 'data/merged/' + datelog_dir + '/'
    
    #mapping_file = 'TestAQMHealthInfoQuestions'
    
    #mapping_file = 'AQMPersonalQuestionsV1'
    #mapping_file = 'AQMGeneralQuestionsV1' 
    #mapping_file = 'AQMHealthInfoQuestionsV1'
    mapping_file = 'AQMHouseholdQuestionsV1'
    
    #mapping_file = 'AQMMentallHealthQuestionsV1'
    #mapping_file = 'AWHDisabilityQuestionsV1'
    
    
    resident_file = merged_dir +  mapping_file + '.csv'
    parsed_output_csv_file = merged_dir +  mapping_file + '.tmp.csv'
    parsed_output_json_file = merged_dir + mapping_file + '.output.json'
    '''
    
    resident_file           = 'data/merged/couchbase-curis-2019-06-21-cuartero/health_information.csv'
    parsed_output_csv_file  = 'data/merged/couchbase-curis-2019-06-21-cuartero/health_information.tmp.csv'
    parsed_output_json_file = 'data/merged/couchbase-curis-2019-06-21-cuartero/health_information.output.json'
    '''
    
    #resident_file           = 'data/merged/couchbase-curis-2019-06-21-cuartero/TestAQMHealthInfoQuestions.csv'
    #parsed_output_csv_file  = 'data/merged/couchbase-curis-2019-06-21-cuartero/TestAQMHealthInfoQuestions.tmp.csv'
    #parsed_output_json_file = 'data/merged/couchbase-curis-2019-06-21-cuartero/TestAQMHealthInfoQuestions.output.json'
    


# # BUG LIST
# ## 1. if file have no 'id' header

# ## 2. Fix "nan" on final rendering
# ## 3. Rename 'id' as _id

# In[625]:


resident_df = pd.DataFrame()
resident_df = pd.read_csv(resident_file, encoding = "ISO-8859-1") 
#hc_df.reset_index(level=0, inplace=True)

##ERROR HERE IF '' == np.nan
#resident_df = resident_df.replace(np.nan,'',regex=True)
resident_df.head(3)


# In[626]:


resident_df.T


# ## BUG: if _index is not the first df.index

# In[627]:


def concat_values(items):
    _list = []
    _index_map = ''
    _id = ''
    
    for index ,value in items.iteritems():
        
        _new_item = ''
        
        ##TODO: Check unqiue identifer here
        if index == "_id":
        #if index == "id":
            _id = value
            _list.append(_id)
           
        elif index == "_index_map":
            _index_map = value
            
            _list.append(_index_map)
            #TODO: MUST BE A FUNCTION --> Parser of index_map
        else:
            if len(index) > 1 :
                
                _new_index = index.split(sep='.')
                
                if pd.isna(_index_map) :
                    _new_index[0] = _new_index[0]
                else:
                    _new_index[0] = _new_index[0] + '|' + str(_index_map)
                    
                _edited_index = '.'.join(_new_index)
                _edited_index

                ## REMOVE np.nan
                if pd.isna(value):
                    _new_item = _edited_index + ':""'
                else:
                    _new_item = _edited_index + ":" + str(value) + ""
                
                
                
                _list.append(_new_item)
            else:
                print('contineu..')
                continue
            
    return _list    


# In[635]:


new_residents_df = pd.DataFrame()
new_resident_df = resident_df.T.apply(concat_values)
new_resident_df = new_resident_df.drop(index='_index_map')

#TODO: Change idnetifier _id formid id accordingly
#new_resident_df = new_resident_df.drop(index='_id')
new_resident_df


# In[629]:


def insert_concat(items):
    test_df = pd.DataFrame()
    _test_obj = []
    _test_id = ''
    for index ,value in items.iteritems():
        _kv = {}
        
        #TODO: change _id here in formid, id or id
        if index == '_id':
            #print(index)
            _test_id = value
            continue
        else:
            _kv["_id"] = _test_id
            _kv["value"] = value
            
        _test_obj.append(_kv)
    return _test_obj


# In[630]:


vertical_resident_df = []
vertical_resident_df = new_resident_df.apply(insert_concat)

flat = list(itertools.chain.from_iterable(vertical_resident_df))
flat_df = pd.DataFrame.from_dict(flat)
flat_df.head(13)


# ## WRITE TO CSV FILE

# In[631]:


#TODO: delete if exisitng
flat_df.to_csv(parsed_output_csv_file, encoding='utf-8', mode='w', index=False)


# In[632]:


hc_df = pd.DataFrame()
hc_df = pd.read_csv(parsed_output_csv_file,header=None, dtype=str) 
hc_df.head(10)


# ## TODO BUG: if _id is not in index[0]

# In[633]:


def setValue(value, field, it):
    if isinstance(value, dict) or isinstance(value, list) and field not in it: # prevent override
            it[field] = value
    if not isinstance(value, dict) and not isinstance(value, list):
            it[field] = value

def findField(ds, k, keyVal, it):
    it = ds[k]
    if isinstance(it, list): # if array then find correct index
        index = int(keyVal[1])
        if len(it) > index:
            it = it[index]
    return it

def setAttribute(profile, lastKeys, field, value):
    it = {}
    for key in lastKeys:
        keyVal = key.split('|')
        k = keyVal[0]
        if not it:
            it = findField(profile, k, keyVal, it)
        else:
            it = findField(it, k, keyVal, it)

    if isinstance(it, list):
        index = int(lastKeys[-1].split('|')[1]) # get index from ['contactnumber/0']
        if len(it) > index: # find element and element attribute to set value
            setValue(value, field, it[index])
        else: # list is empty so create an element
            for i in range(len(it), index + 1, 1):
                it.append({})
            it[index][field] = value
    else:
        setValue(value, field, it)

def defineField(profile, field):
    keyVal = field.split('|')
    if len(keyVal) > 1:
        key = keyVal[0]
        if key not in profile:
            profile[key] = []
    elif field not in profile:
        profile[field] = {}

profiles = []

def properValue(value):
    proper = None
    try:
        proper = eval(value)
    except:
        proper = value
    return proper

# app starts here


print('parser running...')
with open(parsed_output_csv_file, encoding="utf-8") as csv_file:
#with open(resident_file, encoding="utf-8") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    profile = {}
    lastId = None
    firstProfile = True
    for row in csv_reader: # read the csv line by line
        if line_count > 0: # don't read columns
            # print(row[1].rsplit(':', 1))
            fieldValueRaw = row[1]
            fieldValue = fieldValueRaw.split(":", 1)
            rawFields = [x.strip() for x in fieldValue[0].split('.')]
            rawFields[-1] = rawFields[-1] + ':' + fieldValue[1]
            # stop updating profile when id changes
            # add profile and build it afterwards
            
            if lastId != row[0]: # new id means add and build profile
                if not firstProfile:
                    profile = {} # reset profile and build it
                profile['id'] = row[0]
                profiles.append(profile) # this profile will be built below
                lastId = row[0]
                firstProfile = False

            lastKeys = []
            for field in rawFields: # build the profile up
                keyVal = field.split(':', 1) # field and value format, we separate it
                if len(keyVal) == 1: # ['answers'] if field doesn't contain value we build it
                    if len(lastKeys) == 0:
                        defineField(profile, field)
                    else:
                        _keyVal = field.split('|')
                        if len(_keyVal) > 1:
                            setAttribute(profile, lastKeys, _keyVal[0], [])
                        else:
                            setAttribute(profile, lastKeys, field, {})
                    lastKeys.append(field)
                else: # ['type', 'mobile']
                    
                    if len(lastKeys) == 0:
                        profile[keyVal[0]] = properValue(keyVal[1])
                    else:
                        setAttribute(profile, lastKeys, keyVal[0], properValue(keyVal[1]))
                    lastKeys.append(keyVal[0])
        line_count += 1

#os.makedirs('output', exist_ok=True)
#file = open(parsed_output_json_file, 'w')
#file.write(json.dumps(, indent=4, sort_keys=True))
#file.write(json.dumps(profiles))
#file.close()

#encoding = "ISO-8859-1"
with open(parsed_output_json_file, 'w', encoding="utf-8") as outfile:
#with open(parsed_output_json_file, 'w', encoding="ISO-8859-1") as outfile: #For KHM
    json.dump(profiles, outfile, indent=4, sort_keys=True, ensure_ascii=False)
    
print('parser completed...')


# ## REMOVE TEMPORARY FILE

# In[ ]:


if os.path.exists(parsed_output_csv_file):
    os.remove(parsed_output_csv_file)

