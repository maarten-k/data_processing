#!/usr/bin/python
# 
# 
############################################################
############################################################

# Import Libraries & Setup

############################################################
############################################################


# Import required modules
import random, os, re, sys
import couchdb, credentials
import gridtools, gridtools_extended
from random import shuffle
import sqlite3
from sqlite3 import Error


# Connect to CouchDB
db = gridtools.connect_to_couchdb(credentials.URL, credentials.USERNAME, credentials.PASS, credentials.DBNAME)


####################################

# couchDB Functions

####################################


# Fetch tokens with state
def fetch_view_state(view, state):
	tokenIDs = []
	for row in db.iterview(view + str("/" + state), 1000):
		tokenIDs.append(row['id'])
	return(tokenIDs)


# Parse db
def parseTokens(tokenIDs):

	# Initialize
	out = []
	pat1 = re.compile('MD5SUMS-CHECK:\t')
	pat2 = re.compile('FLAGSTATS-CHECK:\t')

	# Try and iteratively parse tokens
	for tokenID in tokenIDs:

		try:

			# Fetch token log
			token = db.get(tokenID)
			tokenID = str(tokenID.replace('_Realignment_Checks', ''))
			token = token['log'][2]['message']['stdout'].split('\n')

			# Parse md5sum status
			md5sums = filter(pat1.findall, token)
			md5sums = [ str(i.split('\t')[1]) for i in md5sums ]
			md5sums = len(re.findall('cram: OK', str(md5sums)))

			# Parse flagstats
			flagstats = filter(pat2.findall, token)
			read1 = int(str(flagstats[6]).split('\t')[1].split(' ')[0])
			read2 = int(str(flagstats[7]).split('\t')[1].split(' ')[0])
			flagstats = read1 + read2

			# Append tuple to upload
			token = (tokenID, md5sums, flagstats)
			out.append(token)

		# Print error if parse returns any error
		except:
			print('Error parsing token:\t' + tokenID)
	return(out)


####################################

# SQLite Functions

####################################


# Function to create database
def create_connection(db):
    """ create a database connection to a SQLite database """
    con = None
    try:
        con = sqlite3.connect(db)
    except Error as e:
        print(e)
    finally:
        if con:
            con.close()


# Function to create database
def create_checks_db(db):
	create_connection(db)
	query_fq = str('create table if not exists Realignment_Checks(IID varchar(250), Final_MD5sum int(10), Read_Count int(20), Primary Key (IID));')
	con = sqlite3.connect(db)
	cur = con.cursor()
	cur.execute(query_fq)
	con.commit()
	return(str('Realignment_Checks tables created into ' + db))


# Function to return a field from tbl to a list
def fetch_list(tbl, col, db):
	query = str('select ' + col + ' from ' + tbl + ';')
	create_connection(db)
	con = sqlite3.connect(db)
	cur = con.cursor()
	cur.execute(query)
	uploaded = []
	for i in cur.fetchall():
		uploaded.append(str(i[0]))
	return(uploaded)


# Check table
def check_tbl(db, tbl):
	create_connection(db)
	con = sqlite3.connect(db)
	try:
		query = str('select * from ' + tbl + ' limit 10;')
		cur = con.cursor()
		cur.execute(query)
		out = []
		for i in cur.fetchall():
			out.append(i)
	except Error:
		out = str('Error inserting updates for ' + tbl)
	return(out)


# Function to import tuple
def import_checks(db, updates):
	create_connection(db)
	con = sqlite3.connect(db)
	try:
		cur = con.cursor()
		cur.executemany("insert into Realignment_Checks (IID, Final_MD5sum , Read_Count) values (?, ?, ?)", updates)
		con.commit()
		out = check_tbl(db, 'Realignment_Checks')
	except Error:
		out = str('Error inserting updates for Realignment_Checks')
	return(out)


#################################################

# Parse Logs and Import Tuple

#################################################


# Fetch done tokens from view
view = "Realignment_Checks"
state = "done"
tokens = fetch_view_state(view, state)
upload = parseTokens(tokens)
len(upload)

# Create table for import
db2 = str('/home/bkenna/project/databases/meta.db')
create_checks_db(db2)

# Filter for new records
tbl = "Realignment_Checks"
col = "IID"
old = fetch_list(tbl, col, db2)
updates = []
for i in upload:
	if i[0] not in old:
		updates.append(i)

# Impot new data
import_checks(db2, updates)
