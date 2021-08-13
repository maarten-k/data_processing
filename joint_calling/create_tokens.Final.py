#!/usr/bin/python2.7


# Load Required Modules
import os, sys, re
from zlib import *

import gridtools
import credentials
import couchdb


# Set Main Args
view = sys.argv[1]
genome = sys.argv[2]
projectID = sys.argv[3]
soft = str("/cvmfs/softdrive.nl/projectmine_sw/software/bin/data_processing/joint_calling")


# Connect to couchDB
credentials.VIEW_NAME = view
db = gridtools.connect_to_couchdb(url=credentials.URL, username=credentials.USERNAME, password=credentials.PASS, dbname=credentials.DBNAME)


# Handle views
if view == 'Shard_gVCF':

	# Skip token if exists
	SM = sys.argv[4]
	gvcf = sys.argv[5]
	tgt = sys.argv[6]
	tokenname = str(SM + "_" + projectID + "_Shard_gVCF")
	if tokenname in db:
		print("Skipping " + tokenname + " found in " + credentials.DBNAME)

	else:

		# Set token fields
		gvcf_adler = gridtools.get_adler32_on_srm(gvcf)
		gvcfDict = {"url": gvcf, "adler32": gvcf_adler}

		# Write task script
		taskScript = """bash ${soft}/Setup-genoDB-gVCFs.sh ${SM} ${genome} ${projectID} ${gvcf} ${tgt}"""
		taskScript = gridtools.templater(taskScript)

		# Write token
		token = {
			"_id": tokenname,
			"lock": 0,
			"done": 0,
			"type": view,
			"Genome": genome,
			"files": gvcfDict,
			"Task_ID": SM,
			"Task_Script": taskScript
		}

		# Upload token
		print("Added Token = " + tokenname + " to Database = " + credentials.DBNAME + " under View =  " + credentials.VIEW_NAME)
		db.save(token)



# Handle Joint Calling
elif view == "JointCalling":

	# Skip token if exists
	tgt = sys.argv[4]
	shard = sys.argv[5]
	genome = sys.argv[6]
	taskID = str(taskID + "_" + shard)
	tokenname = str(taskID + "_JointCalling")
	if tokenname in db:
		print("Skipping " + tokenname + " found in " + credentials.DBNAME)

	else:

		# Set token fields
		gvcf_list = DataFile
		data_adler = gridtools.get_adler32_on_srm(gvcf_list)
		dataDict = {"url": gvcf_list, "adler32": data_adler}

		# Write task script
		taskScript = """bash ${soft}/Generate-Callset.sh ${SM} ${tgt} ${gvcf_list} ${genome}"""
		taskScript = gridtools.templater(taskScript)

		# Write token
		token = {
			"_id": tokenname,
			"lock": 0,
			"done": 0,
			"type": view,
			"files": dataDict,
			"Loci": tgt,
			"Shard": shard,
			"Task_ID": taskID,
			"Genome": genome,
			"Task_Script": taskScript
		}

		# Upload token
		print("Added Token = " + tokenname + " to Database = " + credentials.DBNAME + " under View =  " + credentials.VIEW_NAME)
		db.save(token)


# Exit
else:
	print("\nExiting no valid WGS view provided\n")
