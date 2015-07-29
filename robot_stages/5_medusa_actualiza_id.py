#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pymongo
from pymongo import MongoClient
import pprint
import time
import pandas as pd


pp = pprint.PrettyPrinter(indent=1)
client = MongoClient()
client = MongoClient('localhost', 27017)
db_aemet = client.AEMET
db_croja = client.CRUZROJA
col_playas = db_croja['playas']
col_localidades = db_aemet['localidades']
col_costas = db_aemet['costas']
today = time.strftime("%Y-%m-%d")


#Número de localidades
resultado_localidades  = col_localidades.find({})
print "Hay %d localidades" % resultado_localidades.count()

#número de playas de la aemet
resultado_costas = col_costas.find({})
print "Hay %d costas" % resultado_costas.count()

#Número de playas de la cruz roja
resultado_playas = col_playas.find({})
print "Hay %d playas" % resultado_playas.count()


# Vamos a cargar los ids de las costas en la en la bbdd de mongo de playas
# Lo primero es cargar el csv que tiene los ids, luego filtrar por aquellos que tienen el id relleno y por
# último actualizar los registros en la bbdd de playas

df_playas = pd.DataFrame.from_csv('playas_cruzroja.csv',)
df_playas = df_playas.dropna()

#y ahora inserto en mongo en la db = CRUZROJA , coleccion = playas , el id_costa para el _id de la playa en el dataframe
for id_cr in df_playas.index:
    print id_cr
    id_playa = id_cr
    id_costa = unicode(int(df_playas.ix[id_cr].id_costa))
    costa = {}
    costa["id_costa"] = id_costa
    post_id = col_playas.update({"_id" : id_playa},{ "$set" : costa}, upsert=True)
    print post_id




#Hago una query de comprobación (saco unicamente un registro):
playas  = col_playas.find({"id_costa": { "$exists" : True }},
                                  {"id_costa":1})
for playa in playas:
    id_costa = str(playa["id_costa"]).zfill(7)
    costas = col_costas.find({"_id": unicode(id_costa)}, 
                             {"longitud":1,
                              "latitud":1})

    for costa in costas:
        playa['latitud'] = costa['latitud']
        playa['longitud'] =  costa['longitud']
        id_playa = playa["_id"]
        del playa["_id"]
        ret = col_playas.update({"_id" : id_playa},{ "$set" : playa}, upsert=True)