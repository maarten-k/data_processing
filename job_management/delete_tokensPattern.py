#!/usr/bin/python2.7

#######################################################
#######################################################


# Import modules
import sys, os, re
import credentials, gridtools, gridtools_extended
view = sys.argv[1]
pattern = re.compile(sys.argv[2])
print("\n\nPattern to query:\t" + sys.argv[2] + "\n")


#######################################################
#######################################################


# Connect to couchdb and fetch tokens matching pattern
db = gridtools.connect_to_couchdb(url=credentials.URL, username=credentials.USERNAME, password=credentials.PASS, dbname=credentials.DBNAME)
for state in [ 'todo', 'locked', 'done' ]:

	# Fetch tokens & filter
	tokenIDs = gridtools_extended.getTokenIDs(view, state)
	toClear = filter(pattern.findall, tokenIDs)

	# Clear tokens
	print("Clearing tokens from view " + view + " state " + state + " N = " + str(len(toClear)) + "\n")
	for i in toClear:
		token = db.get(i)
		db.delete(token)
