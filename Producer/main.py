#!/usr/bin/python3                                                                                                      
                                                                                                                        
from kafka import KafkaProducer                                                                                            
import os
import pandas as pd
import json
import requests
from sys import argv, exit
import sys
from time import time, sleep
from urllib.parse import urlencode
from cassandra.util import uuid_from_time
from datetime import datetime

def read_city(filename):
    filename_path = os.path.join(main_path,filename)
    data = pd.read_excel(filename_path)
    return data['City'].tolist()

API_KEY = 'f9a30a29b5f243bda2565626222412'
main_path = '/mnt/c/diary ngoding/Test Telkom/weather kafka ingestion/Producer/'
BROKER = 'localhost:9092'
CITY_PROFILES = []
CITY_PROFILES = read_city('list-cities-indonesia-63j.xlsx')
print(CITY_PROFILES)


try:                                                                                                                    
    p = KafkaProducer(bootstrap_servers=BROKER)                                                                         
except Exception as e:                                                                                                  
    print(f"ERROR --> {e}")                                                                                             
    sys.exit(1)

while True:
	for x in CITY_PROFILES:
		try:
			url = ''
			response1 = ''
			msg = ''
			d = {}

			
			WEATHER_URL = 'http://api.weatherapi.com/v1/forecast.json?'
			mydict = {'key': API_KEY, 'q': x, 'days': 1, 'aqi': 'no', 'alerts': 'no'}
			url = WEATHER_URL + urlencode(mydict, doseq=True)
			response1 = requests.get(url)
			d = json.loads(response1.text)
			uuid = uuid_from_time(datetime.now())
			#print(uuid)
			msg = f"{uuid},{d['location']['name']},{d['location']['localtime_epoch']},{d['location']['localtime']},{d['current']['last_updated_epoch']},{d['current']['last_updated']},{d['current']['temp_c']},{d['current']['temp_f']},{d['current']['wind_mph']},{d['current']['wind_kph']},{d['current']['wind_degree']}"
			print(msg)
			p.send('weather', bytes(msg, encoding="utf8"))
			sleep(0.5)
		except:
			print("api tidak ada :" + x)
			pass
	sleep(15)