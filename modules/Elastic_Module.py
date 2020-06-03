#!/usr/bin/env python
# coding: utf-8

# In[16]:


from ssl import create_default_context
from elasticsearch import Elasticsearch
import pandas as pd
from time import gmtime, strftime
import sys
from pandas.io.json import json_normalize
import numpy as np

# In[5]:


#ES_HOST = "https://es01:9200"


# In[6]:




# In[11]:
class elastic_connection:
    
    def __init__(self,ES_HOST,basic_auth=None,ssl_context=None):
    
        if ssl_context is not None:
            context = create_default_context(cafile=ssl_context['cafile'])
            es = Elasticsearch(
            [ES_HOST],
            http_auth=(ssl_context['user'], ssl_context['psswd']),
            scheme="https",
            port=443,
            ssl_context=context)
            self.es=es
         
        if basic_auth is not None:
            self.es= Elasticsearch(ES_HOST, http_auth=(basic_auth['user'], basic_auth['psswd']))

    
    def ping(self):
        print(self.es.ping())
        
    def list_indices(self):
        #listar todos los índices de elasticsearch
        return list(self.es.indices.get_alias().keys())
        
    def read_all_index(self,INDEX_NAME='',n=1000,to_df=True,rw=True):
        es=self.es
        res = es.search(index = INDEX_NAME,size=n,body={"query": {"match_all": {}}})
        if to_df==True:
            res=self.query_to_df(res)
        if rw==True:
            self.docs=res
        else:
            return res
        
    def search(self,INDEX_NAME,size=1000,to_df=True,rw=True,query={}):
        es=self.es
        res=es.search(index = INDEX_NAME,size=size,body=query)
        if to_df==True:
            res=self.query_to_df(res)
        if rw==True:
            self.docs=res
        else:
            return res
        
        
    def add_docs(self,docs):
        self.docs=docs
        
    def query_to_df(self,res):
        #https://stackoverflow.com/questions/25186148/creating-dataframe-from-elasticsearch-results
        df = json_normalize([x['_source'] for x in res['hits']['hits']])
        return df
    
    def test_index(self,INDEX_NAME,n=15,as_df=True):
        es=self.es
        # sanity check
        res = es.search(index = INDEX_NAME,size=n, body={"query": {"match_all": {}}})
        if as_df==True:
            return self.query_to_df(res)
        else:
            return(res)
        
        
    def count_documents(self,INDEX_NAME):
        es=self.es
        es.indices.refresh(INDEX_NAME)
        res=es.cat.count(INDEX_NAME, params={"format": "json"})
        print(res)
        
        
    def get_indices(self):
        es=self.es
        print(es.indices.get_alias("*"))
        
    def bulk_data(self,INDEX_NAME='',_type='',n=1500,id_field=''):
        df=self.docs
        print('creando paquetes de data bulk')
        #generate bulk data packages for indexing, input== pandas DF
        if type(df)!=list:
            data=df.to_dict(orient='records')
        else:
            data=df
        bulk_data=[]
        for idx in range(0,len(data)):
            if len(id_field)>0:
                _id=data[idx][id_field]
            else:
                _id=idx
            bulk_data.append({"index":{ "_index" : INDEX_NAME, "_type" : _type, "_id" : _id}})
            doc=data[idx]
            doc.pop(id_field, None)
            bulk_data.append(doc)
        l=bulk_data
        self.chunks=[l[i:i + n] for i in range(0, len(l), n)]
        self.size_chunks=len(self.chunks)
        self.ndocs=0
        for c in self.chunks:
            self.ndocs+=len(c)/2
        self.size_docs=sys.getsizeof(self.chunks)/1024
        print('size of bulk docs is : '+str(self.size_docs)+' Kbs')
        print('N° docs: '+str(self.ndocs)+', N° chunks: '+str(self.size_chunks))
        
        
    # bulk index the data
    def bulk_index(self,debug_doc=False):
        chunks=self.chunks
        es=self.es
        INDEX_NAME_=chunks[0][0]['index']['_index']
        print("bulk indexing...:"+'index---> '+INDEX_NAME_)
        exceptions=[]
        es_errors=[]
        for idx in range(0,len(chunks)):
        
            try:
                print("\r", 'indexing chunk : '+str(idx)+' of '+str(len(chunks)) , end="")
                res = es.bulk(index = INDEX_NAME_, body = chunks[idx],refresh = True)
                errors=self.elastic_errors(res)
                es_errors.append(errors)

            except Exception as e:
                ex={'exception':str(e),'timeStamp':strftime("%Y-%m-%d %H:%M:%S"),'index':INDEX_NAME_,
             'init_id':chunks[idx][0]['index']['_id'],'end_id':chunks[idx][len(chunks[idx])-2]['index']['_id'],
                   'ndocs':len(chunks[idx])/2,
                '_id':strftime("%Y_%m_%d_%H_%M_%S")+str(chunks[idx][0]['index']['_id'])}
                exceptions.append(ex)
                continue
        print('finished bulk indexing')
        es_errors=[x for x in es_errors if x!=None]
        if len(es_errors)>0:
            es_errors=pd.concat(es_errors) #ES indexing errors
            if es_errors.shape[0]>0:
                self.es_errors=es_errors
                print(str(self.es_errors.shape[0])+' ES indexing errores, see index "es_errors"')
                
        else:
            print('No recorded ES indexing errors')
        if len(exceptions)>0:
            exceptions=pd.DataFrame(exceptions) #exceptions
            if exceptions.shape[0]>0:
                self.exceptions=exceptions
                self.exceptions['exception'].replace("\'",'"') #remove linespace from exceptions strings
                print(str(self.exceptions.shape[0])+' exceptions while indexing to Elasticsearch, see index "exceptions"')
        else:
            print('No recorded ES indexing exceptions')
                
        if (len(exceptions)>0) & (len(es_errors)>0):
            er=elastic_errors(self)
            er.index_errors()

    def elastic_errors(self,res):
        status=[]
        ids=[]
        errors_type=[]
        errors_reason=[]
        indexes=[]
        timeStamp=[]
        _ids=[] #IDs con los que van a ser indexados los errores
        if res['errors']==True:
            for i in res['items']:
                if i['index']['status']!=201:
                    status.append(i['index']['status'])
                    ids.append(i['index']['_id'])
                    errors_type.append(i['index']['error']['type'])
                    errors_reason.append(i['index']['error']['reason'])
                    indexes.append(i['index']['_index'])
                    timeStamp.append(strftime("%Y-%m-%d %H:%M:%S"))
                    _ids.append(strftime("%Y_%m_%d_%H_%M_%S")+str(len(_ids)))
            
        if len(status)>0:
            errors=pd.DataFrame([_ids,status,ids,errors_type,errors_reason,indexes,timeStamp],index=['_id','status','doc_id','error_type','error_reason','index','timeStamp']).T
        else:
            errors=pd.DataFrame([],columns=['_id','status','doc_id','error_type','error_reason','index','timeStamp'])

    def clear_index(self,INDEX_NAME):
        es=self.es
        if es.indices.exists(INDEX_NAME):
            print("deleting '%s' index..." % (INDEX_NAME))
            res = es.indices.delete(index = INDEX_NAME)
            print(" response: '%s'" % (res))
        # since we are running locally, use one shard and no replicas
        request_body = {
        "settings" : {
            "number_of_shards": 1,
        "number_of_replicas": 0
            }
        }
        print("creating '%s' index..." % (INDEX_NAME))
        res = es.indices.create(index = INDEX_NAME, body = request_body)
        print(" response: '%s'" % (res))
        
    def custom_id(self,d,fields=[],sep='',head=7):
        #concatenar serie de columnas para generar string para id
        df=d
        df['_id']=''
        for f in fields:
            #limitado a 'head' número de carácteres por campo
            df['_id']=df['_id']+sep+df[f].astype(str).str[:head]
        df['_id']=df['_id'].replace(' ','-')
        return df

    def generate_id(self,d,x='',y=''):
        df=d
        df[y]=df.index.astype(str)+df[x]
        df[y]=df[y].replace({':':'-'})
        return df       
        
    def elastic_prepare_pattern(self,rw=True):
        docs=self.docs.copy()
        #generar ids para elastic
        if self.has_attribute('generateID_params'): #nativos,a partir de algún campo '_id'
            docs=self.generate_id(docs,**self.generateID_params)
        if self.has_attribute('generateID_params_custom'): #a partir de concatenar varios campos
            docs=self.custom_id(docs,**self.generateID_params_custom)
        #patrón de retornar documentos o nada (y guardar como atributo de la instancia), según parametro 'rw' (read-write)
        docs=self.data_integrity()

        return self.read_write_pattern(docs,rw=rw)
    
    def data_integrity(self):
        #asegurar integridad de data para elastic
        docs=self.docs.copy()
        docs=docs.replace({np.nan:None})
        docs=docs.replace({float('nan'):None})
        docs=docs.astype(object).where(pd.notnull(docs),None)      
        
        return docs
    
    def has_attribute(self,attribute):
        return (hasattr(self, attribute))
    
    def read_write_pattern(self,docs,rw=True):
        #patrón de retornar documentos o nada (y guardar como atributo de la instancia), según parametro 'rw' (read-write)
        if rw==True:
            self.docs=docs
            return(None)
        else:
            return docs
        
class elastic_errors(elastic_connection):
    
    def __init__(self,es):
        self.__dict__={item: value for (item, value) in es.__dict__.items() if item not in ['data','chunks'] }
        

    def index_errors(self):

        def index_(es,i,data):
            n=1500
            id_field='_id'
            self.add_docs(data)
            self.bulk_data(INDEX_NAME=i[0],_type=i[1],n=n,id_field=id_field)
            self.bulk_index()

        if self.has_attribute('es_errors'):
            i=('es_errors','errors') 
            data=self.es_errors
            index_(es,i,data)

        if self.has_attribute('exceptions'):
            i=('exceptions','exceptions') 
            data=self.exceptions
            index_(es,i,data)          