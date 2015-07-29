#!/usr/bin/python
# -*- coding: UTF-8 -*-

import pymongo
from pymongo import MongoClient
import time
import os
import tweepy
import requests
import pprint

ckey = 'AudDS2qWKad8bnQhytqozVV9p'
consumer_secret = 'nGApgLkYWN5qjVnDk1LFY307XbKk3x6LP2z8TmdMBaqvS1XLdo'
access_token_key = '3296532819-xSeTwH2AO8SExtRQcg3p7KYPVJYY6F4wxzbbhCT'
access_token_secret = '3XnFOBl0CBZ2uCESsKxKyoZee9eBgf1jfRvxW9nYxpeeE'


auth = tweepy.OAuthHandler(ckey, consumer_secret)
auth.secure = True
auth.set_access_token(access_token_key, access_token_secret)

# access the Twitter API using tweepy with OAuth
api = tweepy.API(auth) 


client = MongoClient()
client = MongoClient('localhost', 27017)

db_croja = client.CRUZROJA
col_playas = db_croja['playas']

today = time.strftime("%Y-%m-%d")

# Obtengo los que tienen medusas para el día de hoy con código de costa.
playas = col_playas.find( 
    { 
        "id_costa": 
         { 
            "$exists": True
        },
        today+ ".medusas": {"$exists": True},
        "provincia": {
            "$in":[
                "Cantabria",
                "Valencia",
                "Castellon",
                "Alicante"
            ]
        }
    }, 
    { 
        "2015-07-23.medusas":1,
        today+".medusas": 1, 
        "provincia":1, 
        "nombre_playa":1,
        "municipio": 1,
        "latitud":1,
        "longitud":1
    } 
)

tweets_remaining = 1

ahora = time.strftime("%Y-%m-%dT%H:%M:%SZ")

for playa in playas:
    tweet = "Pinchando la sombrilla playa de %s, %s (%s) :) | %s" % (
        playa["nombre_playa"].encode("utf8"), 
        playa["municipio"].encode("utf8"), 
        playa["provincia"].encode("utf8"),
        ahora
    )
    
    if playa[today]["medusas"]== u"Sí" and tweets_remaining:
        print playa
        continue
        fn = os.path.abspath('./imagenes/jelly_twitter_test.jpg')
        #UpdateStatus of twitter called with the image file
        a = api.update_with_media(
            fn, 
            status=str(tweet)
        )
        tweets_remaining -= 1