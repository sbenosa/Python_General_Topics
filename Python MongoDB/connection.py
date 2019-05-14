"""
Created on May 05, 2019
@author: Sherwin Benosa
#############################################################################
MONGODB CONNECTION ALL RECORDS WILL BE VERIFID ON MONGO DB COMPASS CONNECTION
#############################################################################
"""
class connection:
	from pymongo import MongoClient
	client = MongoClient('localhost', port=27017)

	def __init__(self, dbname):
		self.db = connection.client[dbname]
		
	def getDB(self):
		return self.db