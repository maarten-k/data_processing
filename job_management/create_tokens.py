#!/usr/bin/python2.7


# Load Required Modules
import os, sys, re
from zlib import *

import gridtools
import credentials
import couchdb


# Set Main Args
view = sys.argv[1]
DataFile = sys.argv[2]
taskID = sys.argv[3]
SM = taskID
soft = str("${soft}/data_processing")


# Connect to couchDB
credentials.VIEW_NAME = view
db = gridtools.connect_to_couchdb(url=credentials.URL, username=credentials.USERNAME, password=credentials.PASS, dbname=credentials.DBNAME)


# Handle views
if view == 'Alignment':

	# Skip token if exists
	build = sys.argv[4]
	tokenname = str(SM + "_" + build + "_Alignment")
	if tokenname in db:
		print("Skipping " + tokenname + " found in " + credentials.DBNAME)

	else:

		# Set token fields
		bam = DataFile
		bam_adler = gridtools.get_adler32_on_srm(bam)
		bamDict = {"url": bam, "adler32": bam_adler}

		# Write task script
		taskScript = """bash ${soft}/Realignment.sh ${bam} ${SM} ${build}"""
		taskScript = gridtools.templater(taskScript)

		# Write token
		token = {"_id": tokenname,
			"lock": 0,
			"done": 0,
			"type": view,
			"files": bamDict,
			"Task_ID": SM,
			"Task_Script": taskScript
		}

		# Upload token
		print("Added Token = " + tokenname + " to Database = " + credentials.DBNAME + " under View =  " + credentials.VIEW_NAME)
		db.save(token)


# Read Processing tokens
elif view == "Dedup_BQSR":

	# Skip token if exists
	tokenname = str(SM + "_Dedup_BQSR")
	if tokenname in db:
		print("Skipping " + tokenname + " found in " + credentials.DBNAME)

	else:

		# Set token fields: bam = BWA-CRAM
		bam = DataFile
		bam_adler = gridtools.get_adler32_on_srm(bam)
		bamDict = {"url": bam, "adler32": bam_adler}

		# Write task script
		taskScript = """bash ${soft}/Dedup-BQSR.sh ${bam} ${SM}"""
		taskScript = gridtools.templater(taskScript)

		# Write token
		token = {"_id": tokenname,
			"lock": 0,
			"done": 0,
			"type": view,
			"files": bamDict,
			"Task_ID": SM,
			"Task_Script": taskScript
		}

		# Upload token
		print("Added Token = " + tokenname + " to Database = " + credentials.DBNAME + " under View =  " + credentials.VIEW_NAME)
		db.save(token)


# Realignment Checks tokens
elif view == "Realignment_Checks":

	# Skip token if exists
	tokenname = str(SM + "_Realignment_Checks")
	if tokenname in db:
		print("Skipping " + tokenname + " found in " + credentials.DBNAME)

	else:

		# Set token fields
		bam = DataFile
		try:
			bam_adler = gridtools.get_adler32_on_srm(bam)
		except:
			bam_adler = "tmp"
		bamDict = {"url": bam, "adler32": bam_adler}

		# Write task script
		taskScript = """bash ${soft}/sanity-check.sh ${bam} ${SM}"""
		taskScript = gridtools.templater(taskScript)

		# Write token
		token = {"_id": tokenname,
			"lock": 0,
			"done": 0,
			"type": view,
			"files": bamDict,
			"Task_ID": SM,
			"Task_Script": taskScript
		}

		# Upload token
		print("Added Token = " + tokenname + " to Database = " + credentials.DBNAME + " under View =  " + credentials.VIEW_NAME)
		db.save(token)


# Variant Calling tokens: 	Temporialy redirected to Testing locally
elif view == "Variant_Calling":

	# Skip token if exists
	build = sys.argv[4]
	genome = sys.argv[5]
	tokenname = str(SM + "_" + build + "_Variant_Calling")
	if tokenname in db:
		print("Skipping " + tokenname + " found in " + credentials.DBNAME)

	else:

		# Set token fields
		bam = DataFile
		bam_adler = gridtools.get_adler32_on_srm(bam)
		bamDict = {"url": bam, "adler32": bam_adler}

		# Write task script
		if genome == "WXS":
			taskScript = """bash ${soft}/HaplotypeCaller.sh ${bam} ${SM} ${build} ${genome}"""
		else:
			taskScript = """bash ${soft}/HaplotypeCaller-WGS.sh ${bam} ${SM} ${build} ${genome}"""

		taskScript = gridtools.templater(taskScript)

		# Write token
		token = {"_id": tokenname,
			"lock": 0,
			"done": 0,
			"type": "VariantCalling_HaplotypeCaller_V2",
			"files": bamDict,
			"Task_ID": SM,
			"Task_Script": taskScript
		}

		# Upload token
		print("Added Token = " + tokenname + " to Database = " + credentials.DBNAME + " under View =  " + credentials.VIEW_NAME)
		db.save(token)

# Exit
else:
	print("\nExiting no valid WGS view provided\n")
