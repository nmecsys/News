# -*- coding: utf-8 -*-
"""
Created on Fri Mar  9 10:54:54 2018

@author: jonatha.costa
"""

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
#from elasticsearch_dsl import Search
from pymongo import MongoClient
import pandas as pd 
import numpy as np
import requests
import json
import time
from cryptography.fernet import Fernet



#criando a conexão com o mongodb

def get_news_mongo(collection,datainicial = None,datafinal = None):
    file = open('key_mongo.key','rb')
    key = file.read()
    file.close()
    f = Fernet(key)
    credenciais = pd.read_table('crendialsENCRIPT_mongo.txt').columns[0].encode()
    credenciais = f.decrypt(credenciais).decode()
    client = MongoClient(credenciais)
    
    db = client.iiebr_info
    aux = db.folha_online
    #gt =  > , lt =  <
    if(datainicial == None and datafinal == None):
        cursor = aux.find()
        df = pd.DataFrame.from_records(cursor)
    elif(datainicial == None):    
        cursor = aux.find({"date":{'$lt':datafim}})
        df = pd.DataFrame.from_records(cursor)
    elif(datafinal == None):
        cursor = aux.find({"date":{'$gt':datainicio}})
        df = pd.DataFrame.from_records(cursor)
    else:
        cursor = aux.find({"date":{'$gt':datainicio}},{"date":{'$lt':datafim}})
        df = pd.DataFrame.from_records(cursor)
    return(df)




def get_results(link):
    file = open('key_elasticsearch.key','rb')
    key = file.read()
    file.close()
    f = Fernet(key)
    credenciais = pd.read_table('crendialsENCRIPT.txt').columns[0].encode()
    r = requests.get(credenciais + '/folha_impresso/noticias/_search?q=_url:"'+str(link)+'"')
    results = json.loads(r.text)
    tem = results['hits']['total']
    return(tem)

def tratando(df,index):
   file = open('key_elasticsearch.key','rb')
   key = file.read()
   file.close()
   f = Fernet(key)
   credenciais = pd.read_table('crendialsENCRIPT.txt').columns[0].encode()
        
   credenciais = f.decrypt(credenciais).decode()

   es = Elasticsearch(credenciais,timeout=60,max_retries=10)
    
   if es.indices.exists(index=index):
       insert_elasticsearch(index,df)
       for i in range(len(df)):
           aux = get_results(df['link'][i])
           print(i)
       if(aux==0):
           df.drop(df.index[[i]])
    return(df)    

k = tratando(df = df,index = 'folha_online')

k = np.array_split(df,3)
m = k[0]
tmp = m.to_json(orient = "records")
n = k[1]
tmp = df.to_json(orient = "records")
o = k[2]
tmp = df.to_json(orient = "records")



df = pd.read_csv('m.csv',encoding='utf-8')

    
def insert_elasticsearch(index,df): 
     tmp = df.to_json(orient = "records")
     index_name = index
     type_name = 'noticias'
     file = open('key_elasticsearch.key','rb')
     key = file.read()
     file.close()
     f = Fernet(key)
     credenciais = pd.read_table('crendialsENCRIPT.txt').columns[0].encode()
        
     credenciais = f.decrypt(credenciais).decode()

     es = Elasticsearch(credenciais,timeout=60,max_retries=10)
     actions = [
           {
           '_index' : index_name,
           '_type' : type_name,
           '_categoria' : df['categoria'],
           '_url' : df['link'],
           '_publication_date' : df['date'],
           '_title': df['manchete'],
           '_text': df['noticia'],
            }
      for df in json.loads(tmp)
      ]
     bulk(es, actions)
     return('Foi inserido'+'em'+str(index))
      
    
    

    
