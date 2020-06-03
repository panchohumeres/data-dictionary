#!/usr/bin/env python
# coding: utf-8

# In[8]:


import pymongo
import types
import datetime
from bson.objectid import ObjectId
import pandas as pd
from pymongo import MongoClient
from importlib import reload
import numpy as np
import sys


class unwind:
    
    def __init__(self,**kwargs):
        self.__dict__.update(**kwargs)
        
        
        self.docs=self.docs[0]#select one record
        self.docs=self.flatten(sep=self.sep) #flatten nested data
        self.docs=self.df_expand(sep=self.sep)    
            

    def flatten(self,sep="_"):
        d=self.docs
        import collections

        obj = collections.OrderedDict()

        def recurse(t,parent_key=""):
        
            if isinstance(t,list):
                for i in range(len(t)):
                    recurse(t[i],parent_key + sep + str(i) if parent_key else str(i))
            elif isinstance(t,dict):
                for k,v in t.items():
                    recurse(v,parent_key + sep + k if parent_key else k)
            else:
                obj[parent_key] = t

        recurse(d)
        
        self.flat=dict(obj)

        return dict(obj)
    
    def df_expand(self,sep='_'):
        df=pd.DataFrame(pd.Series(self.docs))
        df.columns=['value']
        df['key']=df.index.astype(str)
        expanded=df['key'].str.split(sep,expand=True)
        keys=[]
        for col in expanded.columns:
            keys.append('key('+str(col)+')')
        expanded.columns=keys
        df=pd.concat([df,expanded],axis=1)
        df['description']=''
        df['type']=df['value'].map(lambda x: str(type(x)).replace("<class '","").replace("'>",""))
                
        return df
     