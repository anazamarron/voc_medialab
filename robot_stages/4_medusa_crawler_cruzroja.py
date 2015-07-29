#!/usr/bin/python
# -*- coding: UTF-8 -*-

from scrapy import Spider, Request, Item, Field
from scrapy.selector.lxmlsel import Selector
from scrapy import log
import json
from pymongo import MongoClient
import time

class MedusaItem(Item):
    comunidad = Field()
    playa = Field()
    id = Field()

class MedusaCrawler(Spider):
    name= "MedusaCrawler"
    
    start_urls = ["http://www.cruzroja.es/appjv/consultaplayas/listaPlayas.do"]
    
    playa_url = "http://www.cruzroja.es/appjv/consultaplayas/fichaPlaya.do"
    
    townid_xpath = "//html/body/table/tr[4]/td/table/tr[3]/td[2]/table/tr"
    
    playa_xpath = "//html/body/table/tr[5]/td/table/tr[2]/td/table"
    
    client = MongoClient('localhost', 27017)
    db = client.CRUZROJA
    collection = db['playas']
    
    def build_request(self, url, type):
        log.msg(url, level=log.ERROR)
        return Request(url, callback = lambda response:  self.parse_response(response, type))
        
    
    def parse(self, response):
        hxs = Selector(response)
        
        # find town id
        rows = hxs.xpath(self.townid_xpath)
        for row in rows:
            row = row.xpath('./td[3]/input/@onclick').extract()
            try:
                row = int(row[0].split('(')[1].split(')')[0])
            except:
                continue
            
            yield self.callback_playa(self.playa_url+"?id=%d" % row, row, self.parse_playa)
            
    
    def callback_playa(self, url, idplaya, callback):
        return Request(url, callback=lambda response: callback(response, idplaya))

    
    def parse_playa(self, response, idplaya):
        log.msg(response.url, log.DEBUG)
        hxs = Selector(response)
        
        playa_xpath = {
                       'nombre_playa': './tr[1]/td/table/tr[1]/td[2]/div/text()',
                       'provincia': './tr[1]/td/table/tr[2]/td/div[1]/ul/li[2]/text()',
                       'municipio': './tr[1]/td/table/tr[2]/td/div[2]/ul/li[2]/text()',
                       'cobertura_desde': './tr[1]/td/table/tr[2]/td/div[3]/ul/li[2]/text()',
                       'cobertura_hasta': './tr[1]/td/table/tr[2]/td/div[3]/ul/li[4]/text()',
                       'horario': './tr[1]/td/table/tr[2]/td/div[4]/ul/li[2]/text()',
                       'bandera': './tr[2]/td/div/fieldset/div[1]/div[1]/ul/li[2]/img/@alt',
                       'sello_aenor_iso_9001': './tr[2]/td/div/fieldset/div[1]/div[2]/ul/li[3]/text()',
                       'sello_aenor_iso_14001': './tr[2]/td/div/fieldset/div[1]/div[3]/ul/li[3]/text()',
                       'cantidad_puestos': './tr[2]/td/div/fieldset/div[1]/div[4]/ul/li[3]/text()',
                       'sillas_proximidad': './tr[2]/td/div/fieldset/div[1]/div[5]/ul/li[3]/text()',
                       'cantidad_torres_vigilacia': './tr[2]/td/div/fieldset/div[1]/div[6]/ul/li[3]/text()',
                       'cantidad_torres_intervencion': './tr[2]/td/div/fieldset/div[1]/div[7]/ul/li[3]/text()',
                       'medusas': './tr[2]/td/div/fieldset/div[1]/div[8]/ul/li[3]/text()',
                       'ayuda_banio': './tr[2]/td/div/fieldset/div[1]/div[9]/ul/li[3]/text()',
                       'frecuencia_atencion': './tr[2]/td/div/fieldset/div[1]/div[10]/ul/li[3]/text()',
                       'atencion_discapacitados': './tr[3]/td/div/fieldset/div[1]/div[1]/ul/li[2]/text()',
                       'acceso_discapacitados': './tr[3]/td/div/fieldset/div[1]/div[2]/ul/li[2]/text()',
                       'rampas': './tr[3]/td/div/fieldset/div[1]/div[3]/ul/li[2]/text()',
                       'servicios_wc': './tr[3]/td/div/fieldset/div[1]/div[4]/ul/li[2]/text()',
                       'duchas': './tr[3]/td/div/fieldset/div[1]/div[5]/ul/li[2]/text()',
                       'vestuarios': './tr[3]/td/div/fieldset/div[1]/div[6]/ul/li[2]/text()',
                       'parking': './tr[3]/td/div/fieldset/div[1]/div[7]/ul/li[2]/text()',
                       'sombra': './tr[3]/td/div/fieldset/div[1]/div[8]/ul/li[2]/text()',
                       'cantidad_sillas_adaptadas': './tr[3]/td/div/fieldset/div[2]/div[2]/ul/li[2]/text()',
                       'pasarelas': './tr[3]/td/div/fieldset/div[2]/div[3]/ul/li[2]/text()',
                       'atencion_cre': './tr[3]/td/div/fieldset/div[2]/div[4]/ul/li[2]/text()',
                       'atencion_familiar': './tr[3]/td/div/fieldset/div[2]/div[5]/ul/li[2]/text()',
                       'horario_atencion': './tr[3]/td/div/fieldset/div[2]/div[1]/ul/li[2]/text()',
                       'lunes': './tr[3]/td/div/fieldset/div[2]/div[1]/ul/li[3]/table/tr[2]/td[1]/text()',
                       'martes': './tr[3]/td/div/fieldset/div[2]/div[1]/ul/li[3]/table/tr[2]/td[2]/text()',
                       'miercoles': './tr[3]/td/div/fieldset/div[2]/div[1]/ul/li[3]/table/tr[2]/td[3]/text()',
                       'jueves': './tr[3]/td/div/fieldset/div[2]/div[1]/ul/li[3]/table/tr[2]/td[4]/text()',
                       'viernes': './tr[3]/td/div/fieldset/div[2]/div[1]/ul/li[3]/table/tr[2]/td[5]/text()',
                       'sabado': './tr[3]/td/div/fieldset/div[2]/div[1]/ul/li[3]/table/tr[2]/td[6]/text()',
                       'domingo': './tr[3]/td/div/fieldset/div[2]/div[1]/ul/li[3]/table/tr[2]/td[7]/text()',
                       'consejo': '//html/body/table/tr[5]/td/table/tr[4]/td/text()'
                       }
        
        rows = hxs.xpath(self.playa_xpath)
        for row in rows:
            playa = {}
            for key in playa_xpath.keys():
                try:
                    playa[key] = row.xpath(playa_xpath[key]).extract()[0]
                except:
                    playa[key] = ''
                playa['id'] = idplaya
        
        today = time.strftime("%Y-%m-%d")
        doc = {today: playa,
               'nombre_playa': playa['nombre_playa'],
               'municipio': playa['municipio'],
               'provincia': playa['provincia']
               }
        
        del playa['nombre_playa']
        del playa['municipio']
        del playa['provincia']
        
        self.collection.update({"_id": idplaya},{"$set": doc}, upsert=True)
        
