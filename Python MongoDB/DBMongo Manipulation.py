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

def main():

# #+----------------------------+
# #|	  CALL DB CONNECTION      |
# #+----------------------------+
	obj = connection.connection('top_movies')
	db = obj.getDB()
	movies = db.movies
   	
	n = 1
#query last movie        
	print ('\nquery last', n, 'movie/s')
	length = movies.find().count()
	last_n = movies.find().skip(length - n)
	for row in last_n:
		print (row)
        
#define variables to search 
	releaseDate = ['Dec 18, 2009']
	movieName   = ['Tangled', 'Spectre','Avatar']   
 
#table name | field name | variable name
	print ('\nquery Tangled:')
	query_1st_in_list = movies.find( {'Movie':{'$in':[movieName[0]]}})
	for row in query_1st_in_list:
		print (row)
                
#query based on the given fields (AND)         
	print ('\nquery using Release Date AND Movie Name:')
	query_and = movies.find( {'Release_Date':releaseDate[0], 'Movie':movieName[2]} )
	for row in query_and:
		print (row)
        
#query either of the two fields (OR)       
	print ('\nquery either of the movies in variable:')
	query_or = movies.find( {'$or':[{'Movies':movieName[0]},
	{'Release_Date':releaseDate[0]}]} )
	for row in query_or:
		print (row)
        
# #query using pattern (like function in SQL)     
	patterns = '^Avengers'
	print ('\nquery using patterns like', patterns, 'movies')
	query_like = movies.find( {'Movies':{'$regex':patterns}} )
	for row in query_like:
		print (row)

#Insert new record 
	pid = movies.count()
	doc = {'_id':pid, 'Release_Date':'May 05, 2019', 'Movie':'The Best Movie','Production_Budget':'5000',
	'Domestic_Gross':'1600000000','Worldwide_Gross':'99999999999999999'}
	movies.insert_one(doc)
	print ('\ndisplay added document:')
	q_added = movies.find({'Movie':'The Best Movie'})
	print (q_added.next())

if __name__ == '__main__':
    main()
