#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pymongo
from pymongo import MongoClient
import time
import os
from ftplib import FTP
import pprint
import requests


def normalize_text(text):
    '''
        Elimina signos de puntuación
        Elimina tildes
        Cambia espacios por guiones
        Convierte la cadena a minúscula
    '''
    import unicodedata

    norm_text = ''.join((c for c in unicodedata.normalize('NFD',text.decode('utf-8')) if unicodedata.category(c) != 'Mn'))
    norm_text_out = ''
    hyphen = 0
    for char in norm_text.encode('utf-8'):
        if str.isalnum(char):
            norm_text_out += char
            hyphen = 0
        else:
            if hyphen == 0:
                norm_text_out += '-'
                hyphen = 1
    return norm_text_out.strip('-').lower()


client = MongoClient()
client = MongoClient('localhost', 27017)

db_aemet = client.AEMET
db_croja = client.CRUZROJA
col_playas = db_croja['playas']
col_localidades = db_aemet['localidades']
col_costas = db_aemet['costas']
today = time.strftime("%Y-%m-%d")

#Hago una query de comprobación (saco unicamente un registro):
playas  = col_playas.find({"id_costa": { "$exists" : True }},
                             {"nombre_playa":1,
                              "id_costa":1,
                              "municipio":1,
                              "provincia":1,
                              "latitud":1,
                              "longitud":1,
                               today+".medusas": 1})

paths = "/path/to/xml"
docs_buscador = []

for playa in playas:
    doc = {}
    if today in playa:
        
        id_costa = str(playa["id_costa"]).zfill(7)
        
        costas = col_costas.find({"_id": unicode(id_costa)}, 
                                     {"nombre_playa":1, 
                                      "nombre_localidad":1, 
                                      "nombre_provincia":1 , 
                                      "longitud":1,
                                      "latitud":1})
        
        doc['idplaya'] = playa['_id']
        #print "############################"
        #pprint.pprint(playa)
        for costa in costas:
            doc['latitud'] = costa['latitud']
            doc['longitud'] =  costa['longitud']
            doc['nombre_playa'] = playa['nombre_playa']
            doc['pois'] = "%s,%s" % (playa['latitud'], playa['longitud'])
            doc['titulo'] = "Lorem Ipsum Dolor sit "
            doc['cuerpo'] = "Hoy %s hay medusas" % playa[today]['medusas']
        
        
        doc['medusas'] = playa[today]['medusas']
        doc['path'] = "/sociedad/playas/%s-%s-%s.html" % (
            playa['_id'],
            normalize_text(playa['provincia'].encode('utf8')) , 
            normalize_text(playa['nombre_playa'].encode('utf8'))
        )
        
        docs_buscador.append(doc)
        

url_publicacion = "http://des.buscador.srv.vocento.in/krammer/playas/publish"

xml = '''<?xml version="1.0" encoding="utf-8"?>
<playa>
    <idplaya>%s</idplaya>
    <longitud>%s</longitud>
    <latitud>%s</latitud>
    <pois>%s</pois>
    <titulo>%s</titulo>
    <cuerpo>%s</cuerpo>
    <path>%s</path>
</playa>
'''
for doc in docs_buscador:
    xmlpublish = xml %(doc['idplaya'], doc['longitud'], doc['latitud'], doc['pois'], doc['titulo'], doc['cuerpo'], doc['path'])
    if u"Sí" == doc['medusas']:
        print xmlpublish
    #r = requests.post(url_publicacion, data=xmlpublish)
    #print r.status_code
    #print xmlpublish