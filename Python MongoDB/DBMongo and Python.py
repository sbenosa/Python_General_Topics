"""
Created on May 05, 2019
@author: Sherwin Benosa
#############################################################################
This program will read CSV file and convert records to dictionary. It will
again converted to JSON file and will insert each record into a noSQL MongoDB
#############################################################################
"""

import json, csv, sys, os
sys.path.append(os.getcwd()+'/classes')
import connection 

def read_dict(file, hdr):
	input_file = csv.DictReader(open(file), fieldnames=hdr)
	return (input_file)

def conv_reg_dict(rec):
	return [dict(x) for x in rec]
 
def dump_json(file, rec):
	with open(file, 'w') as f:
		json.dump(rec, f)

def read_json(file):
	with open(file) as f:
		return json.load(f)


def main():
#+----------------------------+
#|	    READ CSV INPUT        |
#+----------------------------+
	file = 'movies.csv'
	headers = ['Release_Date','Movie','Production_Budget','Domestic_Gross','Worldwide_Gross']
	r_dict = read_dict(file, headers)

#+----------------------------+
#|	  CONVERT CSV TO DICT     |
#+----------------------------+
	dict_ls = conv_reg_dict(r_dict)

#+----------------------------+
#|	 CONVERT DICT TO JSON     |
#+----------------------------+
	json_file = 'movies.json'
	dump_json(json_file, dict_ls)

#+----------------------------+
#|	      READ JSON           |
#+----------------------------+
	data = read_json(json_file)

#+----------------------------+
#|	  CALL DB CONNECTION      |
#+----------------------------+
	obj = connection.connection('top_movies')
	db = obj.getDB()
	movies = db.movies
	movies.drop()

#+----------------------------+
#|	  INSERT ALL RECORDS      |
#+----------------------------+
	for i, row in enumerate(data):
		row['_id'] = i
		movies.insert_one(row)
 
#validate first n record       
	n = 3
	print('1st', n, 'top_movies:')
	people = movies.find()
	for i, row in enumerate(people):
		if i < n:
			print (row)

#validate n record after rewind              
	people.rewind()	
	print('\n1st', n, 'top movies with rewind:')
	for i, row in enumerate(people):
		if i < n:
			print (row)
 
# #validate if n record is inserted           
	print ('\nquery 1st', n, 'records')
	first_n = movies.find().limit(n)
	for row in first_n:
		print (row)
     
# #query last 3 names        
	print ('\nquery last', n, 'records')
	length = movies.find().count()
	last_n = movies.find().skip(length - n)
	for row in last_n:
		print (row)

if __name__ == '__main__':
    main()
