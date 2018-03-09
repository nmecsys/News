# -*- coding: utf-8 -*-
"""
Created on Fri Mar  9 10:54:54 2018

@author: jonatha.costa
"""

import pandas as pd 
import numpy as np 
import pyes 
import json
import 

df = pd.read_csv("mediacloud_export.csv",sep=";",decimal=".")

df["no_index"] = [x+1 for x in range(len(df["id"]))]

tmp = df.to_json(orient = "records")

df_json= json.loads(tmp)
print df_json[0]

index_name = 'estadao'
type_name = 'noticias'
es= pyes.ES()




for doc in df_json:
    es.index(doc, index_name, type_name, id=doc['no_index'])