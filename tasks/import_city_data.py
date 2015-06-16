#!/usr/bin/python

import sys
import pymongo
import csv

config = kaptan.Kaptan(handler='yaml')
config.import_config('../config.yml')

MONGO_USER = config.get('mongo_user')
MONGO_PASS = config.get('mongo_pass')
MONGODB_URI = config.get('mongo_uri')

client = pymongo.MongoClient(MONGODB_URI % (MONGO_USER, MONGO_PASS))
db = client.get_default_database()

def main(args):
		
		city_records = []

		# load csv
		with open('la-principle-cities.csv', 'rb') as csvfile:
			cities = csv.reader(csvfile, delimiter=',')
			
			for row in cities:
				
				# create a city object
				city = {
					'name': row[0],
					'zipcode': row[1],
					'latlong': row[2],
					'population': int(row[3])
				}

				city_records.append(city)
		
		save_to_databse(city_records)

		client.close()


def save_to_databse(data):
		
		db.drop_collection('cities')

		cities = db['cities']
		cities.insert(data)

		print cities.find().count()

		for city in cities.find():
			print city


if __name__ == '__main__':
		main(sys.argv[1:])