#!/usr/bin/python
# -*- coding: UTF-8 -*-

from lxml import html
import requests
import json
import pandas as pd

import pymongo
from pymongo import MongoClient

def update_localidades_mongo(collection, localidades):
	#Inserto las localidades en Mongo. Esto solo hace falta hacerlo una vez, a partir de ahora empiezo con las previsiones
	for clave in localidades:
		#Convertimos el texto a json para que mongo lo acepte
		d = json.loads(localidades[clave])
		del d['_id']
		post_id = collection.update({u"_id" : clave},{"$set" :d},upsert=True) 

client = MongoClient()
client = MongoClient('localhost', 27017)
db = client.AEMET
collection = db['localidades']


url = "http://www.aemet.es/es/eltiempo/prediccion/municipios"

page = requests.get(url)
tree = html.fromstring(page.text)

#Cargamos el excel con las coordenadas de las localidades
titulo = "municipios-espana-coordenadas-2011.csv"
df = pd.read_csv(titulo,index_col=0)



provincias = dict()

#Saco las provincias de la página
provincias_name = tree.xpath('//select[@id="provincia_selector"]/option/text()')
provincias_id = tree.xpath('//select[@id="provincia_selector"]/option/@value')

#Creo un dataFrame con las provincias y su id para la AEMET
#provincias = pd.DataFrame()
for i in range (0,len(provincias_id)-1):
	provincias[provincias_id[i+1]] = {"nombre":provincias_name[i+1]}
	
#Voy a obtener un diccionario con todas las localidades de españa y la provincia a la que pertenecen
localidades = dict()
import datetime

for key in provincias:
	url_municipios = "http://www.aemet.es/es/eltiempo/prediccion/municipios?p="+ key
	page_municipio = requests.get(url_municipios)
	tree_municipio = html.fromstring(page_municipio.text)
	localidades_name = tree_municipio.xpath('//select[@id="localidades_selector"]/option/text()')
	localidades_id = tree_municipio.xpath('//select[@id="localidades_selector"]/option/@value')
	for i in range(0,len(localidades_id)-1):
		nombre_municipio = localidades_name[i+1]
		coordenadas = pd.DataFrame()
		nombre_provincia = provincias[key]['nombre'].encode('utf_8')
		#Hago un try porque hay poblaciones que no se encuentran en el excel, habrá que modificarlas a mano
		try:									
			
			nombre_coordenadas = df.ix[nombre_municipio.encode('utf_8')]
			if nombre_coordenadas.provincia == nombre_provincia:
				coordenadas = df.ix[nombre_municipio.encode('utf_8')]
		except:
			#print df.ix[nombre_municipio]
			#print "falla: " , nombre_municipio , ": ", nombre_provincia
			coordenadas["longitud"]= ""
			coordenadas["latitud"] = ""
			coordenadas["comunidad"] = ""
			
		#creo el objeto json que voy a asignar al diccionario:

		texto = '{ "_id": "' + localidades_id[i+1][-5:] + '" '
		texto += ', "nombe_localidad" : "'+ localidades_name[i+1] + '" '
		texto += ', "id_localidad" : "' + localidades_id[i+1] + '" '
		texto += ', "id_provincia" : "' +  key  + '" '
		texto += ', "nombre_provincia": "' +   provincias[key]['nombre'] + '" '
		if not coordenadas.empty:
			texto += ', "comunidad" : "' +  coordenadas.comunidad.decode('utf_8') + '" '
	   
			texto += ', "longitud": "' +  str(coordenadas.longitud) + '"'
	   
			texto += ', "latitud": "' + str(coordenadas.latitud) + '"'
		texto += ', "date" : "' + str(datetime.datetime.utcnow()) + '" '
		texto += '}'
   
		
		if localidades_id[i+1][-5:] in localidades:
			localidades[localidades_id[i+1][-5:]].append(texto)
			
		else: 
			localidades[localidades_id[i+1][-5:]] =texto
			
	
update_localidades_mongo(collection, localidades)
