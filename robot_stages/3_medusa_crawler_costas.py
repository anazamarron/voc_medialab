#!/usr/bin/python
# -*- coding: UTF-8 -*-

from lxml import html
import requests
import json
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
import re

#Mongo
import pymongo
from pymongo import MongoClient

client = MongoClient()
client = MongoClient('localhost', 27017)
db = client.AEMET

# Añado un dataframe con los meses para la fecha
global meses 
df_meses = pd.DataFrame()
df_meses["es"] = ["enero","febrero","marzo","abril","mayo","junio","julio","agosto"
				,"septiembre","octubre","noviembre","diciembre"]
df_meses["en"] = ["january","february", "march", "april","may","june","july","august"
				,"september","october","november", "december"]
df_meses["num"] = ["01","02", "03", "04","05","06","07","08","09","10","11", "12"]

def traduceMes(mes,lang_origen,lang_destino):
	for index, row in meses.iterrows():
		if(mes.upper().find(row[lang_origen].upper())==0):
			mes = mes.upper().replace(row[lang_origen].upper(),row[lang_destino])
			return mes

def getFechaTime(txt):
	arrayFecha = txt.split(" ")
	mes =  df_meses.ix[df_meses.es==arrayFecha[1].lower()].en
	nueva_fecha =  arrayFecha[0] + mes.values[0] + arrayFecha[2]
	fecha =datetime.strptime(nueva_fecha,'%d%B%Y')
	return fecha

url = "http://www.aemet.es/es/eltiempo/prediccion/playas"
page = requests.get(url)
tree = html.fromstring(page.text)

dict_playas = dict()
total = 0
url_playa = []

#Saco las provincias de la página
provincias_name = tree.xpath('//select[@id="provincia_selector"]/option/text()')
provincias_id = tree.xpath('//select[@id="provincia_selector"]/option/@value')

#Creo un dataFrame con las provincias y su id para la AEMET
#provincias = pd.DataFrame()
for i in range (0,len(provincias_id)-1):
	#provincias[provincias_id[i+1]] = {"nombre":provincias_name[i+1]}
	url_playas = "http://www.aemet.es/es/eltiempo/prediccion/playas?p="+ provincias_id[i+1]
	page_playa = requests.get(url_playas)
	tree_playa = html.fromstring(page_playa.text)
	poblaciones = tree_playa.xpath('//select[@id="datos_selector"]/optgroup/@label')
	poblaciones_id = tree_playa.xpath('//select[@id="datos_selector"]/optgroup/@value')
	
	for poblacion in poblaciones:
		playas =  tree_playa.xpath('//select[@id="datos_selector"]/optgroup[@label="' + poblacion + '"]/option/text()')
		playas_id = tree_playa.xpath('//select[@id="datos_selector"]/optgroup[@label="' + poblacion + '"]/option/@value')
		for playa in playas_id:
			provincias_id[i+1]
			total+=1

			id_playa = playa [len(playa) - 7 : len(playa)]
			nombre_playa = playa[0:len(playa) - 8 ]
			url = "http://www.aemet.es/es/eltiempo/prediccion/playas/"+playa
			url_playa.append(url)
			texto = '{"_id" :"' + id_playa + '"'
			texto += ',"nombre_playa" : "' + nombre_playa + '" '
			texto += ',"id_largo" : "' + playa + '" '
			texto += ',"nombre_localidad": "' + poblacion + '"'
			texto += ',"nombre_provincia": "' + provincias_name[i+1] + '"'
			texto += ',"id_provincia": "' +provincias_id[i+1] + '"}'
			if id_playa in dict_playas:
				dict_playas[id_playa].append(texto)
			else:
				dict_playas[id_playa]= texto
				
print "Leidas %s Playas" % len(dict_playas)

collection = db['costas']
for clave in dict_playas:
	#Convertimos el texto a json para que mongo lo acepte
	d = json.loads(dict_playas[clave])
	if '_id' in d:
		del d['_id']
	post_id = collection.update({u"_id" : clave},{"$set" :d},upsert=True) 
	
resultado  = db.costas.find({"nombre_localidad":"Donostia/San Sebastián"})
for doc in resultado:
	print doc
	
	
#Funciones para convertir las coordenadas
def ConvertDMSToDD(grado, minuto, segundo, direccion):
	dd = float(grado) + float(minuto)/60 + float(segundo)/(60*60)
	if (direccion == "S" or direccion == "O"):
		dd = dd * -1
		# Don't do anything for N or E
	return dd


def ParseDMS(coordenada):
	coordenada = coordenada.strip()
	parts = coordenada.split(" ")
	grado = parts[0].replace("°","_")
	grado = grado[0:len(grado)-1]
	minuto = parts[1][0:len(parts[1])-1]
	segundo = parts [2][0:len(parts[2])-2]
	direccion = parts[3]
	punto = ConvertDMSToDD(grado, minuto, segundo, direccion)
	return punto


print ParseDMS("0° 43' 42'' O")
print ParseDMS("37° 54' 12'' N")


#Empezamos a hacer scrapping de cada una de las playas
playas = dict()
i = 0

print "PLAYAS:"
print "________________________________________________________"
print ""
for url in url_playa:
	#url = 'http://www.aemet.es/es/eltiempo/prediccion/playas/xeraco-4614301'
	try:
		id_playa = url[len(url) - 7:len(url) ]

		texto = '{"_id" :"' + id_playa + '"'
		page_playa = requests.get(url)
		tree_playa = html.fromstring(page_playa.text)
		
		div_cabecera = tree_playa.xpath('//*[@class="notas_tabla"]/text()')
		
		poblacion = div_cabecera[3].strip()	
		fecha_completa = div_cabecera[-1]
		fecha = fecha_completa.split(",")
		fecha_convertida = getFechaTime(fecha[1].strip())
		hoy_es = datetime.strptime(datetime.today().strftime('%Y-%m-%d'),'%Y-%m-%d')

		if fecha_convertida < hoy_es:
			fecha_convertida = hoy_es

		#playa =  tree_playa.xpath('//h3[@class="titulo_fondo_azul"]/text()')
   
		coordenadas = tree_playa.xpath('//span[@class="geo"]/abbr/text()')
		

		lati = coordenadas[0].strip().encode('UTF_8')
		longi = coordenadas[1].strip().encode('UTF_8')


		texto += ' , "longitud": "' + str(ParseDMS(lati)) + '" '
		texto += ' , "latitud": "' + str(ParseDMS(longi)) + '" '
		
		#índice uv
		indice_uv_maximo = tree_playa.xpath('//*[starts-with(@class, "raduv_pred")]/text()')
		#cielo
		cielo = tree_playa.xpath('//table[@class="tabla_datos"][1]/tbody/tr[1]/td/img/@title')
		#Viento
		viento = tree_playa.xpath('//table[@class="tabla_datos"][1]/tbody/tr[2]/td/text()')
		#Oleaje
		oleaje = tree_playa.xpath('//table[@class="tabla_datos"][1]/tbody/tr[3]/td/text()')	
		#temperatura máxima
		temperatura_max = tree_playa.xpath('//table[@class="tabla_datos"][1]/tbody/tr[4]/td/div/text()')
		#sensacion tértmica
		sensacion_termica = tree_playa.xpath('//table[@class="tabla_datos"][1]/tbody/tr[5]/td/text()')
		#temperatura agua
		temperatura_agua = tree_playa.xpath('//table[@class="tabla_datos"][1]/tbody/tr[6]/td/text()')
		#pleamar
		hay_tabla2 = False
		if tree_playa.xpath('//table[@class="tabla_datos"][2]/tbody/tr[1]/td/text()'):
			hay_tabla2 = True
			pleamar =  tree_playa.xpath('//table[@class="tabla_datos"][2]/tbody/tr[1]/td/text()')
			#pleamar
			bajamar =  tree_playa.xpath('//table[@class="tabla_datos"][2]/tbody/tr[2]/td/text()')


		texto += ', "' + str(fecha_convertida) + '": {' 
		texto += ' "temperatura_maxima": ' + temperatura_max[0].strip()
		texto += ', "sensacion_termica": "' + sensacion_termica[0].strip() + '" '
		texto += ', "indice_uv": ' + indice_uv_maximo[0].strip()
		texto += ', "temperatua_agua": ' + temperatura_agua[0].strip()
		texto += ', "cielo_manana": "' + cielo[0].strip() + '" '
		texto += ', "cielo_tarde": "' + cielo[1].strip() + '" '
		texto += ', "viento_manana": "' +  viento[0].strip() + '" '
		texto += ', "viento_tarde": "' +  viento[1].strip() + '" '
		texto += ', "oleaje_manana": "' +  oleaje[0].strip() + '" '
		texto += ', "oleaje_tarde": "' +  oleaje[1].strip() + '" '
		if hay_tabla2:
			texto += ', "pleamar": "' + pleamar[0].strip() + '" '
			texto += ', "bajamar": "' + pleamar[0].strip() + '" '
		texto += '}' 

		fecha_mas_1 = fecha_convertida + timedelta(days=1)

		texto += ', "' + str(fecha_mas_1) + '": {'  
		texto += ' "temperatura_maxima": ' + temperatura_max[1].strip()
		texto += ', "sensacion_termica": "' + sensacion_termica[1].strip() + '" '
		texto += ', "indice_uv": ' + indice_uv_maximo[1].strip()
		texto += ', "temperatua_agua": ' + temperatura_agua[1].strip()
		texto += ', "cielo_manana": "' + cielo[2].strip() + '" '
		texto += ', "cielo_tarde": "' + cielo[3].strip() + '" '
		texto += ', "viento_manana": "' +  viento[2].strip() + '" '
		texto += ', "viento_tarde": "' +  viento[3].strip() + '" '
		texto += ', "oleaje_manana": "' +  oleaje[2].strip() + '" '
		texto += ', "oleaje_tarde": "' +  oleaje[3].strip() + '" '
		texto += '}' 
		texto += '}'

		d = json.loads(texto)
		if '_id' in d:
			del d['_id']
		
		post_id = collection.update({u"_id" : id_playa},{"$set" :d},upsert=True) 
	except:
		i+= 1
		print "error:" ,poblacion , " : " , url 

print str(i) , " errores"