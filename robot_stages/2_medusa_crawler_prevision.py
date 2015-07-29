#!/usr/bin/python
# -*- coding: UTF-8 -*-

from lxml import html
import requests
import json
import pandas as pd
import pprint
import time

import pymongo
from pymongo import MongoClient

client = MongoClient()
client = MongoClient('localhost', 27017)

db = client.AEMET

collection = db['localidades']

pp = pprint.PrettyPrinter(indent=1)

#Vamos a sacar los datos por localidades
for doc in db.localidades.find( { "id_provincia": { "$in": [ "03", "46", "39", "12" ] } } ):
	
	url_prevision = "http://www.aemet.es/xml/municipios/localidad_" + doc['_id'] + ".xml"
	
	try:
		page_prevision = requests.get(url_prevision)
		tree_prevision = html.fromstring(page_prevision.content)
	except:
			print url_prevision

	for fecha in tree_prevision.xpath('//prediccion/dia/@fecha'):
	
		texto = '{"' + fecha + '": {'
	
		#Probabilidad de precipitacion
		hay = False
		texto += '"prob_precipitacion" : {' 
		for periodo in tree_prevision.xpath('//prediccion/dia[@fecha="' + fecha + '"]/prob_precipitacion/@periodo'):
			prob_precipitacion =  tree_prevision.xpath('//prediccion/dia[@fecha="' + fecha + '"]/prob_precipitacion[@periodo="'+periodo+'"]/text()')
			if prob_precipitacion:
				hay = True
				texto += ' "' + periodo + '": ' + prob_precipitacion[0] + ", "
		if hay:
			texto = texto[:-2]
		texto += "}"
			
		#Cota de nieve
		hay = False
		texto += ', "cota_nieve" : {' 
		for periodo in tree_prevision.xpath('//prediccion/dia[@fecha="' + fecha + '"]/cota_nieve/@periodo'):
			cota_nieve =  tree_prevision.xpath('//prediccion/dia[@fecha="' + fecha + '"]/cota_nieve[@periodo="'+periodo+'"]/text()')
			
			if cota_nieve:
				hay = True
				texto += ', "' + periodo + '": ' + cota_nieve[0] + ", "
		if hay:
			texto = texto[:-2]
		texto += "}"

		#Estado del cielo
		hay = False
		texto += ', "estado_cielo" : {' 
		for periodo in tree_prevision.xpath('//prediccion/dia[@fecha="' + fecha + '"]/estado_cielo/@periodo'):
			estado_cielo = tree_prevision.xpath('//prediccion/dia[@fecha="' + fecha + '"]/estado_cielo[@periodo="'+periodo+'"]/@descripcion')				
			if estado_cielo:
				hay = True
				texto += '"' + periodo + '": "' + estado_cielo[0] + '", '
		if hay:
			texto = texto[:-2]
		texto += "}"

		#Direccion del viento
		hay = False
		texto += ', "direccion_viento" : {' 
		for periodo in tree_prevision.xpath('//prediccion/dia[@fecha="' + fecha + '"]/viento/@periodo'):
			direccion_viento = tree_prevision.xpath('//prediccion/dia[@fecha="' + fecha + '"]/viento[@periodo="'+periodo+'"]/direccion/text()')
			if direccion_viento:
				hay=True
				texto += '"' + periodo + '": "' + direccion_viento[0] + '", '
		if hay:
			texto = texto[:-2]
		texto += "}"

			
		#Velocidad del viento
		hay = False
		texto += ', "velocidad_viento" : {' 
		for periodo in tree_prevision.xpath('//prediccion/dia[@fecha="' + fecha + '"]/viento/@periodo'):
			velocidad_viento = tree_prevision.xpath('//prediccion/dia[@fecha="' + fecha + '"]/viento[@periodo="'+periodo+'"]/velocidad/text()')
			if velocidad_viento:
				hay = True
				texto += '"' + periodo + '": ' + velocidad_viento[0] + ", "
		if hay:
			texto = texto[:-2]
		texto += "}"			
		   
			
		#Racha máxima
		hay = False
		texto += ', "racha_max" : {' 
		for periodo in tree_prevision.xpath('//prediccion/dia[@fecha="' + fecha + '"]/racha_max/@periodo'):
			racha_max =  tree_prevision.xpath('//prediccion/dia[@fecha="' + fecha + '"]/racha_max[@periodo="'+periodo+'"]/text()')
			if racha_max:
				hay = True
				texto += '"' + periodo + '": ' + racha_max[0] + ", "
		if hay:
			texto = texto[:-2]
		texto += "}"			

		#Temperatura
		hay = False
		texto += ', "temperatura" : {' 
		maxima = tree_prevision.xpath('//prediccion/dia[@fecha="' + fecha + '"]/temperatura/maxima/text()')
		if maxima:
			hay = True
			texto += '"maxima":'+ maxima[0]+ ", "
		
		minima = tree_prevision.xpath('//prediccion/dia[@fecha="' + fecha + '"]/temperatura/minima/text()')
		if minima:
			hay = True
			texto += '"minima":'+ minima[0] + ", "
		
		for hora in tree_prevision.xpath('//prediccion/dia[@fecha="' + fecha + '"]/temperatura/dato/@hora'):
			por_hora = tree_prevision.xpath('//prediccion/dia[@fecha="' + fecha + '"]/temperatura/dato[@hora="'+hora+'"]/text()')
			if por_hora:
				hay = True
				texto += '"' + hora + '": '+ por_hora[0]+ ", "
		if hay:
			texto = texto[:-2]
		texto += "}"					

		#Sensacion Termica
		hay = False
		texto += ', "sensacion_termica" : {' 
		maxima = tree_prevision.xpath('//prediccion/dia[@fecha="' + fecha + '"]/sens_termica/maxima/text()') 
		if maxima:
			hay = True
			texto += '"maxima":'+ maxima[0]+ ", "
		
		minima = tree_prevision.xpath('//prediccion/dia[@fecha="' + fecha + '"]/sens_termica/minima/text()')
		if minima:
			hay = True
			texto += '"minima":'+ minima[0] + ", "
		
		for hora in tree_prevision.xpath('//prediccion/dia[@fecha="' + fecha + '"]/sens_termica/dato/@hora'):
			por_hora = tree_prevision.xpath('//prediccion/dia[@fecha="' + fecha + '"]/sens_termica/dato[@hora="'+hora+'"]/text()') 
			if por_hora:
				hay = True
				texto += '"' + hora + '": '+ por_hora[0]+ ", "
		
		if hay:
			texto = texto[:-2]
		
		texto += "}"					

		#Humedad relativa
		hay = False
		texto += ', "humedad_relativa" : {' 
		maxima = tree_prevision.xpath('//prediccion/dia[@fecha="' + fecha + '"]/humedad_relativa/maxima/text()') 
		if maxima:
			hay = True
			texto += '"maxima":'+ maxima[0]+ ", "
		
		minima = tree_prevision.xpath('//prediccion/dia[@fecha="' + fecha + '"]/humedad_relativa/minima/text()')
		if minima:
			hay = True
			texto += '"minima":'+ minima[0] + ", "
		
		for hora in tree_prevision.xpath('//prediccion/dia[@fecha="' + fecha + '"]/humedad_relativa/dato/@hora'):
			por_hora = tree_prevision.xpath('//prediccion/dia[@fecha="' + fecha + '"]/humedad_relativa/dato[@hora="'+hora+'"]/text()') 
			if por_hora:
				hay = True
				texto += '"' + hora + '": '+ por_hora[0]+ ", "
		
		if hay:
			texto = texto[:-2]
		texto += "}"					
		
		
		#UV max			
		uv_max =  tree_prevision.xpath('//prediccion/dia[@fecha="' + fecha + '"]/uv_max/text()')
		if uv_max:
			texto += ', "uv_max" : ' +  uv_max[0] 
			
		texto += '}}'

		json_acceptable_string = texto.replace('"', '\"')
		json_acceptable_string = texto.replace("'", "\"")
		
		d = json.loads(json_acceptable_string)

		post_id = collection.update({"_id" : doc["_id"]},{ "$set" : d}, upsert=True)

		time.sleep(0.01)
		
resultado  = db.localidades.find({"nombe_localidad":"Santander"})
for doc in resultado:
	today = time.strftime("%Y-%m-%d")
	poblacion_temperatura = doc[today]["temperatura"]
	for element in poblacion_temperatura:
		print element , ', ' , poblacion_temperatura[element]