#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


def detect_process_errors(df):
    """Function that return the dataframe with added columns in order to conduct the error rate analysis"""
    df = df.sort_values(['client_id','date_time'])
    process_map = {'start':0, 'step_1':1, 'step_2':2, 'step_3':3, 'confirm':4}
    df['process_step_id'] = df['process_step'].map(process_map)
    df['previous_process_step_id'] = (df.sort_values(by=['client_id','date_time']).groupby('client_id')['process_step_id'].shift(1))
    df['is_error'] = df['process_step_id'] < df['previous_process_step_id']
    df_client_error = df[df['is_error']==True].groupby('client_id')['is_error'].agg([max,len])
    return df_client_error


# In[ ]:




