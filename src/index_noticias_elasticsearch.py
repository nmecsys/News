# -*- coding: utf-8 -*-
"""
Created on Fri Mar  9 10:54:54 2018

@author: jonatha.costa
"""

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import pandas as pd 
import json

df = pd.read_csv("mediacloud_export.csv",sep=",")

df["id"] = [x+1 for x in range(len(df["id"]))]
tmp = df.to_json(orient = "records")

index_name = 'estadao'
type_name = 'noticias'


es = Elasticsearch('localhost')

actions = [
     {
     '_index' : index_name,
     '_type' : type_name,
     '_id': df['id'],
     '_website' : df['website'],
     '_url' : df['url'],
     '_publication_date' : df['publication_date'],
     '_title': df['title'],
     '_text': df['text'],
     }
for df in json.loads(tmp)
]

bulk(es, actions)