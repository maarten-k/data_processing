#!/usr/bin/python


########################################################
########################################################


# Import Needed Modules
import gfal2
import gridtools, re, sys, pickle, logging, os
import credentials

import couchdb
import copy

from picas.actors import RunActor
from picas.clients import CouchDB
from picas.iterators import TaskViewIterator
from picas.modifiers import BasicTokenModifier

from zlib import *
from glob import *


########################################################
########################################################

# HOUSEKEEPING FUNCTIONS TO MAKE LIFE EASIER

########################################################
########################################################
	


# Load a file to list
def loadFile(inp):
	"""
	"""

	with open(inp, 'r') as f:
		data = [ line.rstrip('\n') for line in f ]
	f.close()
	return(data)


# Write a list to a file
def writeFile(inp, out):
	"""
	"""

	with open(out, 'w') as f:
		for i in inp:
			f.writelines(str(i))
	f.close()


# Write token logs
def writeLogs(inp, out):
	with open(out, 'w') as f:
		for line in inp:
			f.write(str(line))
	f.close()



########################################################
########################################################

# MAIN FUCNTIONS

########################################################
########################################################


# Create Directory
def createDir(path):

	"""


	Create a directory from dCache if present. Supply the directory path in the srm format.

	Example execution:

		path = "srm://srm.grid.sara.nl:8443/pnfs/grid.sara.nl/data/lsgrid/Project_MinE/myFolder"
		createDir(path)

	"""

	ctx = gfal2.creat_context()

	try:
		ctx.mkdir(str(path))

	except gfal2.GError:
		print "Exiting Directory Already Exists"



# Remove Directory
def removeDir(path):

	"""

	Remove a directory from dCache if present. Supply the directory path in the srm format.

	Example execution:

		path = "srm://srm.grid.sara.nl:8443/pnfs/grid.sara.nl/data/lsgrid/Project_MinE/myFolder"
		removeDir(path)

	"""

	ctx = gfal2.creat_context()

	try:
		ctx.rmdir(str(path))

	except gfal2.GError:
		print "Exiting No Directory Exists"



# List Sample File States: Staged / Unstaged
def FileStateSummary(list):

	"""

	Get the file states for a list of files ie staged / unstaged.

	Example execution:

		list = [ "Sample1.bam", "Sample1.bam.bai", "Sample2.bam", "Sample2.bam.bai" ]
		FileStateSummary(list)

	"""

	ctx = gfal2.creat_context()
	stateSummaries = []
	for SMfile in list:
		SM = os.path.basename(SMfile).split(".")[0]
		status = ctx.getxattr(str(SMfile), "user.status")
		SampleState = str(SM) + "\t" + str(status) + str(SMfile)
		stateSummaries.append(SampleState + "\n")
	return(stateSummaries)



# List Token States for a View
def tokenStates(view):

	"""

	Summarise the token states for a view.

	Example usage:

		view = 'ExtractFastq'
		tokenStates(view)

	"""

	credentials.VIEW_NAME = view
	db = gridtools.connect_to_couchdb(credentials.URL, credentials.USERNAME, credentials.PASS, credentials.DBNAME)
	states = [ "todo", "locked", "done" ]
	stateSummaries = []
	for state in states:
		try:
			ids = []
			for row in db.iterview(credentials.VIEW_NAME + "/" + state, 100):
				ids.append(row['id'])
			var = len(ids)
			data = str(state) + "\t" + str(var) + "\n"
			if data not in stateSummaries:
					stateSummaries.append(data)
		except gridtools.couchdb.http.ResourceNotFound:
			data = str(state) + "\t" + str(0) + "\n"
			if data not in stateSummaries:
				stateSummaries.append(data)
	return(stateSummaries)



# Get Token IDs
def getTokenIDs(view, state):

	"""

	Retrieve a list of token IDs for a view.

	Example execution:

		view = 'ExtractFastq'
		state = 'locked'
		tokenIDs = getTokenIDs(view)

	"""

	credentials.VIEW_NAME = view
	db = gridtools.connect_to_couchdb(credentials.URL, credentials.USERNAME, credentials.PASS, credentials.DBNAME)
	tokenIDs = []

	for row in db.iterview(credentials.VIEW_NAME + str("/" + state), 100):
		tokenIDs.append(row['id'])

	return(tokenIDs)


# Get Metadata & Log File: 	Option to write out the var ?
def tokenLogs(view, tokenID, state):

	"""

	Retrieve metadata and log file for a token as lists.

	Example usage:

		view = "Alignment"
		tokenID = "Con242_Alignment"
		out = "/home/bkenna/project/realignment/TOPMed/alignment"
		token, tokenLogFile = tokenLogs(view, tokenID)

	"""

	# Connect to couchDB
	db = gridtools.connect_to_couchdb(credentials.URL, credentials.USERNAME, credentials.PASS, credentials.DBNAME)
	token = []
	tokenLogFile = []

	# Get Token Data
	SMdata = db.get(tokenID)

	# Get Token Metadata
	if 'wms_job_id' in SMdata:
		tokenKeys = [ '_id', 'Task_ID', 'type', 'files', 'hostname', 'wms_job_id' ]
	else:
		tokenKeys = [ '_id', 'Task_ID', 'type', 'files', 'hostname']
	for i in tokenKeys:
		if i in SMdata:
			token.append(SMdata[i])
			token.append("\n")
		else:
			print("Item " + i + " not found in token " + tokenID + "\n")
			
	# Get Tokens LogFile
	if 'log' in SMdata:
		for i in SMdata['log']:
			tokenLogFile.append(i)
			tokenLogFile.append("\n")
	else:
		print('\nNo log for token:\t' + tokenID + "\n")
	return token, tokenLogFile


# Download attachment for a token
def attachmentFromToken(view, tokenID, out):

	"""
	This functions downloads the attachment from a token.

	Example Execution:
		
		view = 'ExtractFastq'
		tokenID = 'LP6008242-DNA_E03_ExtractFastq'
		out3 = '/home/bkenna/project/realignment/TOPMed/realignment/' + tokenID + '.attachment'
		attachmentFromToken(view, tokenID, out3)

	"""

	# Setup
	credentials.VIEW_NAME = view
	db = gridtools.connect_to_couchdb(credentials.URL, credentials.USERNAME, credentials.PASS, credentials.DBNAME)

	# Get Token Data
	SMdata = db.get(tokenID)

	# Retrieve attachment information if present
	if '_attachments' in SMdata:
		LogFile = list(SMdata['_attachments'].keys())[0]
		LogFile = db.get_attachment(tokenID, LogFile, default=None)
		data = LogFile.read()
		with open(out, 'w') as f:
			f.writelines(str(data))
		f.close()




# Purge tokens from a view: FUNCTION PENDING: Metadata of token left behind raises resource conflict
def purge_tokens(viewname):

	"""
	Purge tokens from the supplied view.

	Example Execution:
		
		view = 'ExtractFastq'
		purge_tokens(view)

	"""

	# Connect to couchdb and setup which documents to delete: Type = view & with a Revision Number
	credentials.VIEW_NAME = view
	db = connect_to_couchdb()
	map_fun = "function(doc) {if (doc.type == '"+viewname+"') emit(doc._id, doc._rev);}"


	# Add tokens from view to the document list for deletion
	docs=[]
	for row in db.query(map_fun):
		docs.append({'_id': row['id'], '_rev': row['value']})
	db.purge(docs)
	print("removed {} documents from view {}".format(len(docs),viewname))



# Create views from list
def generate_views(inp):
	"""
	Create a view for every line in text file.

	Example Execution:

		inp = 'path/to/myViews.txt'
		generate_views(inp)

	"""


	# Connect to CouchDB and Setup View
	db=gridtools.connect_to_couchdb(url=credentials.URL, username=credentials.USERNAME, password=credentials.PASS, dbname=credentials.DBNAME)
	picasitems = {
 
	    "language": "javascript",
	    "views": {
	        "overview": {
	            "map": "function(doc) {\nif(doc[\"type\"]== \"token\"){\n\tif(doc[\"done\"] == 0 && doc[\"lock\"]==0  ){  emit(\"todo\",1);}\n\telse if(doc[\"done\"]== 0 && doc[\"lock\"]>= 1  ){ emit(\"locked\",1);}\n\telse if(doc[\"done\"]>= 1 && doc[\"lock\"]>= 1  ){ emit(\"done\",1);}\n\telse{emit(\"unknown_token_status\",1)}\n}\n\n}",
	            "reduce": "function(key,values,rereduce){return sum(values);}"
	        },
	        "todo": {
	            "map": "function(doc) {\nif(doc[\"type\"]== \"token\"){\n\tif(doc[\"done\"]==0 && doc[\"lock\"]==0  ){\n\t\tif(\"total_chunks\" in doc){\n\t\t\tif (doc[\"total_chunks\"]==doc[\"chunks_ready\"]){\n\t\t\t\temit(doc._id, doc._id);\n\t\t\t}\n\t\t}else{\n\t  \t\temit(doc._id, doc._id);\n\t\t}\n\t\n\t}\n}\n}"
	        },
	        "done": {
	            "map": "function(doc) {\nif(doc[\"type\"]== \"token\"){\n\tif(doc[\"done\"]!= 0){\n  emit(doc[\"_id\"], doc[\"_id\"]);\n}\n}\n}"
	        },
	        "locked": {
	            "map": "function(doc) {\n\tif(doc[\"type\"]== \"token\"){\n\t\tif(doc[\"done\"]== 0 && doc[\"lock\"]>= 1  ){ \n  \t\t\temit(doc[\"_id\"],doc);\n\t\t}\n\t}\n}"
	        },
		"error": {
	           "map": "function(doc) {\n\tif(doc[\"type\"]==\"token\"){if (doc[\"lock\"]==-1 && doc[\"done\"]==-1) {\n\t\t\temit(doc[\"_id\"], doc);\n\t\t}\n\t}\n}"
	       }
	    }
	}



	# Load Views to Create into a List
	with open(inp, 'r') as f:
		myViews = [ line.rstrip('\n') for line in f ]
		f.close()


	# Create View with the above setup
	for stage in myViews:
	    picas_new = copy.deepcopy(picasitems)
	    for key in picas_new["views"].keys():
		        picas_new["views"][key]["map"] = picas_new[
		            "views"][key]["map"].replace("token", stage)
	    print("_design/" + stage)
	    if "_design/" + stage not in db:
	        print("creating view:"+str(stage))
	        db["_design/" + stage] = picas_new
	    del picas_new



# Create a token pool from the supplied file list & upload to view
def CreateTokenPool(inp, viewname):
	"""
	Create & upload token pool for the file list & viename.


	Example Execution:

		inp = 'path/to/mylist.txt'
		viewname = 'test'
		CreateTokenPool(inp, viewname)


	"""


	# Load text file
	with open(inp, 'r') as f:
		myData = [ line.rstrip('\n') for line in f ]
		f.close()

	# Connect to couchDB
	credentials.VIEW_NAME = viewname
	db = gridtools.connect_to_couchdb(url=credentials.URL, username=credentials.USERNAME, password=credentials.PASS, dbname=credentials.DBNAME)


	# Create the token pool
	for i in myData:

		SM = os.path.basename(str(i)).split('.')[0]
		tokenname = SM + "_" + credentials.VIEW_NAME
		myadler32 = adler32(str(i))


		# Skip token if already present else proceed
		if tokenname in db:

			print("Skipping " + SM + " found in " + credentials.DBNAME)

		else:

			token = {"_id": tokenname,
				"lock": 0,
				"done": 0,
				"type": str(credentials.VIEW_NAME),
				"files": { "Data": { "url": i, "adler32" : myadler32} },
				"sample_id": SM,
			}

			print("Added Token = " + tokenname + " to Database = " + credentials.DBNAME + " under View =  " + credentials.VIEW_NAME)
			db.save(token)




# Reset token
def resetToken(tokenID):
	"""
	The purpose of this function is to completely reset a token to the todo state. Please note that any previous log files, attachments, job IDs, error log and hostnames will be permanently deleted if present.

	Example Execution:
		tokenID = 'some_string'
		resetToken(tokenID)
	"""

	# Connect to CouchDB
	db = gridtools.connect_to_couchdb(url=credentials.URL, username=credentials.USERNAME, password=credentials.PASS, dbname=credentials.DBNAME)

	# Fetch token
	if tokenID in db:
		token = db.get(tokenID)
		token['lock'] = '0'
		token['done'] = '0'
		print("Resetted the done and lock fields\n")

		# Drop additional keys that maybe present
		keys = ['wms_job_id', 'hostname', 'error', 'log', '_attachments']
		resetted = []
		for i in keys:
			if i in token:
				del token[i]
				resetted.append(i)
		print("Resetted keys:\t" + str(resetted) + '\n')

		# Update token on couchDB
		try:
			db.save(token)
			print('\nUpdated Token:\t' + tokenID + "\n")
		except (couchdb.http.ResourceNotFound, couchdb.http.ResourceConflict):
			print('\nError Uploading Token:\t' + tokenID + "\n")


# Reset token
def Set_Token_State(tokenID, state):
	"""
	State a tokenID to the specified state.

	Example Execution:
		state = 'todo' || 'locked' || 'done'
		tokenID = 'some_string'
		Set_Token_State(tokenID, state)
	"""

	# Connect to CouchDB
	db = gridtools.connect_to_couchdb(url=credentials.URL, username=credentials.USERNAME, password=credentials.PASS, dbname=credentials.DBNAME)

	# Fetch token
	if tokenID in db:

		# Get token and handle states
		token = db.get(tokenID)
		if state == "todo":
			token['lock'] = '0'
			token['done'] = '0'
		elif state == "locked":
			token['lock'] = '1'
			token['done'] = '0'
		elif state == "done":
			token['lock'] = '1'
			token['done'] = 0

		# Drop additional keys that maybe present
		keys = ['wms_job_id', 'hostname', 'error', 'log', '_attachments']
		resetted = []
		for i in keys:
			if i in token:
				del token[i]
				resetted.append(i)

		# Update token on couchDB
		try:
			db.save(token)
			print('\nUpdated Token:\t' + tokenID + "\n")
		except (couchdb.http.ResourceNotFound, couchdb.http.ResourceConflict):
			print('\nError Updating Token:\t' + tokenID + "\n")


# Set token field
def SetTokenField(tokenID, Keyfield, KeyValue):
	"""
	The purpose of this function is to create a new key and set a value for it.

	Example Execution:
		tokenID = 'some_string'
		KeyField = 'some_key'
		KeyValue = 'some_value_for_key'
		SetTokenField(tokenID, Keyfield, KeyValue)
	"""

	# Connect to CouchDB
	db = gridtools.connect_to_couchdb(url=credentials.URL, username=credentials.USERNAME, password=credentials.PASS, dbname=credentials.DBNAME)

	# Fetch token
	token = db.get(tokenID)
	token[KeyField] = KeyValue

	# Update token on couchDB
	try:
		db.save(token)
		print('\nUpdated Token:\t' + tokenID + "\n")
	except (couchdb.http.ResourceNotFound, couchdb.http.ResourceConflict):
		print('\nError Uploading Token:\t' + tokenID + "\n")
	print("Token field " + KeyField + " set to " + KeyValue + "\n")



# Delete token field
def DeleteTokenField(tokenID, Keyfield):
	"""
	The purpose of this function is to delete a field from a token.

	Example Execution:
		tokenID = 'some_string'
		DeleteTokenField(tokenID, KeyField)
	"""

	# Connect to CouchDB
	db = gridtools.connect_to_couchdb(url=credentials.URL, username=credentials.USERNAME, password=credentials.PASS, dbname=credentials.DBNAME)

	# Fetch token
	token = db.get(tokenID)
	if KeyField in token[KeyField]:
		# Delete field
		print("Deleting field from token:\t" + KeyField)
		del token[KeyField]
		# Update token on couchDB
		try:
			db.save(token)
			print('\nUpdated Token:\t' + tokenID + "\n")
		except (couchdb.http.ResourceNotFound, couchdb.http.ResourceConflict):
			print('\nError Uploading Token:\t' + tokenID + "\n")
	else:
		print("Error: " + KeyField + " not found in token")



# Delete token
def DeleteToken(tokenID):
	"""
	The purpose of this function is to delete a token.

	Example Execution:
		tokenID = 'some_string'
		DeleteToken(tokenID)
	"""

	# Connect to CouchDB
	db = gridtools.connect_to_couchdb(url=credentials.URL, username=credentials.USERNAME, password=credentials.PASS, dbname=credentials.DBNAME)

	# Fetch token
	token = db.get(tokenID)
	db.delete(token)
	print("Deleted token:\t" + tokenID)



# Job summary
def JobSummary(viewName, out):
	"""
	The purpose of this function is to summarise all job states and download all token data locally.
	Log files for the lock and done states are stored in separate directories.

	Example Exection:
		viewName = 'someView'
		out = '/path/to/myResults/Directory'
		JobSummary(viewName, out)

	"""

	# Get attributes
	getTokenIDs = getattri(gridtools_extended, 'getTokenIDs')
	tokenStates = getattri(gridtools_extended, 'tokenStates')
	tokenLogs = getattri(gridtools_extended, 'tokenLogs')
	writeFile = getattri(gridtools_extended, 'writeFile')
	attachmentFromToken = getattri(gridtools_extended, 'attachmentFromToken')

	# Count state of job logs
	print("\nSummarising Token States for Job:\t" + viewName + "\n")
	states = tokenStates(viewName)
	summaries = []
	for i in states:
		summaries.append(i)

	# Fetch done and locked
	if len(out) >= 1:
		# Fetch logs and attachments for completed jobs
		print("\n\nProcessing done tokens:")
		DoneOut = str(out + '/' + viewName + '_done')
		try:
			os.mkdir(DoneOut)
		except OSError:
			print("Output directory exists")
		tokenIDs = getTokenIDs(viewName, 'done')
		done = len(tokenIDs)
		if done > 0:
			print("\nFetching done tokens N = " + str(done))
			for tokenID in tokenIDs:
				token, tokenLogFile = tokenLogs(viewName, tokenID)
				writeFile(token, str(DoneOut + '/' + tokenID + '.token'))
				writeFile(tokenLogFile, str(DoneOut + '/' + tokenID + '.tokenLog'))
				attachmentFromToken(viewName, tokenID, str(DoneOut + '/' + tokenID + '.attachment'))
			print("Logs for done token saved to:\t" + DoneOut)
		else:
			print("\nNo done tokens to fetch\n\n")


		# Fetch logs and attachments for locked jobs
		print("\n\nProcessing locked tokens:")
		LockedOut = str(out + '/' + viewName + '_locked')
		try:
			os.mkdir(LockedOut)
		except OSError:
			print("\nOutput directory exists")
		tokenIDs = getTokenIDs(viewName, 'locked')
		locked = len(tokenIDs)

		if locked > 0:
			print("\nFetching locked tokens N = " + str(locked))
			for tokenID in tokenIDs:
				token, tokenLogFile = tokenLogs(viewName, tokenID)
				writeFile(token, str(LockedOut + '/' + tokenID + '.token'))
				writeFile(tokenLogFile, str(LockedOut + '/' + tokenID + '.tokenLog'))
				attachmentFromToken(viewName, tokenID, str(LockedOut + '/' + tokenID + '.attachment'))
			print("Logs for done token saved to:\t" + LockedOut)
		else:
			print("\nNo locked tokens to fetch")


# Download directory
def downloadRef(path, base=str(0)):
	"""
	"""
	# Create gfal2 context
	ctx = gfal2.creat_context()

	# Download each file: filter for pattern if present
	if '0' not in base:
		pattern = str("*" + base + "*")
		for i in ctx.listdir(path):
			i = list(filter(re.match(pattern, i)))
			if len(i) >= 1:
				base = os.path.basename(i)
				RemoteAdler = ctx.checksum(str(path + "/" + i), "adler32")
				filecopy(str(path + "/" + i), str(os.getcwd() + "/"))
				LocalAdler = adler32(base)

				# Redownload if adler check failed
				if RemoteAdler != LocalAdler:
					os.remove(base)
					filecopy(str(path + "/" + i), str(os.getcwd() + "/"))
					LocalAdler = adler32(base)
	else:
		for i in ctx.listdir(path):
			base = os.path.basename(i)
			RemoteAdler = ctx.checksum(str(path + "/" + i), "adler32")
			filecopy(str(path + "/" + i), str(os.getcwd() + "/"))
			LocalAdler = adler32(base)

			# Redownload if adler check failed
			if RemoteAdler != LocalAdler:
				os.remove(base)
				filecopy(str(path + "/" + i), str(os.getcwd() + "/"))
				LocalAdler = adler32(base)
