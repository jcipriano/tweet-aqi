#!/usr/bin/python

import sys
import pymongo
import csv
import requests
import datetime
import kaptan
from time import sleep 

config = kaptan.Kaptan(handler='yaml')
config.import_config('./config.yml')

MONGO_USER = config.get('mongo_user')
MONGO_PASS = config.get('mongo_pass')
MONGODB_URI = config.get('mongo_uri')

AIRNOW_CURRENT_URI = 'http://www.airnowapi.org/aq/observation/zipCode/current/'
AIRNOW_HISTORICAL_URI = 'http://www.airnowapi.org/aq/observation/zipCode/historical/'
AIRNOW_API_KEY = config.get('airnow_api_key')

AIRNOW_REQ_INTERVAL = 0.25
DATE_START = datetime.datetime(2010,9,1)
OVERWRITE = False

client = pymongo.MongoClient(MONGODB_URI % (MONGO_USER, MONGO_PASS))
db = client.get_default_database()

def main(args):

	cities = db['cities']
	
	# query for all cities with no AQI
	city_query = { 'aqi': None }
	
	# if want to write over everything get all cities
	if OVERWRITE:
		city_query = {}

	# for each city in query results
	for city in cities.find(city_query):

		print "Retrieving AQI data for %s, %s." % (city['name'], city['zipcode'])

		aqi_data = []

		# for each date
		date = DATE_START
		while date.month == DATE_START.month:

			# get and append aqi data
			aqi_data.append( get_aqi_for_day(city['zipcode'], date) )
			
			# increment date
			date += datetime.timedelta(days=1)

			# self rate limiting
			sleep(AIRNOW_REQ_INTERVAL)

		# query for current city
		query = { '_id': city['_id'] }

		# add AQI data to record
		cities.update(query, {
			'$set': {
				'aqi': aqi_data
			}
		})

	client.close()


def get_aqi_for_day(zipcode, date):

	# request and save AQI json data
	resp = get_historical_aqi(zipcode, get_date_string(date))
	resp_json = resp.json()

	result = None
	
	if len(resp_json) > 0:

		# results playload to be returned and saved in DB
		result = {
			'date': resp_json[0]['DateObserved'],
			'ozone': None,
			'pm25': None,
			'pm10': None
		}

		# parse json response and add to payload
		for param in resp_json:

			if param['ParameterName'] == 'OZONE':
				result['ozone'] = param['AQI']

			elif param['ParameterName'] == 'PM2.5':
				result['pm25'] = param['AQI']

			elif param['ParameterName'] == 'PM10':
				result['pm10'] = param['AQI']

	return result


def get_historical_aqi(zipcode, date):

	payload = {
		'format':'application/json',
		'zipCode': zipcode,
		'distance': '25',
		'API_KEY': AIRNOW_API_KEY,
		'date': date
	}

	req = requests.get(url=AIRNOW_HISTORICAL_URI, params=payload)

	return req;


def get_date_string(date):

	return '%s-%s-%sT00-0000' % (date.year, date.month, date.day)


if __name__ == '__main__':
		main(sys.argv[1:])