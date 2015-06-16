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
		
		cities = db['cities']

		print cities.find().count()

		for city in cities.find():
			print "%s,%s,%s,%s" % (city['name'], city['zipcode'], city['latlong'], city['population']) 

		client.close()


if __name__ == '__main__':
		main(sys.argv[1:])