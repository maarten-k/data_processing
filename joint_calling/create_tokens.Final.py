#!/usr/bin/python2.7


# Load Required Modules
import os, sys, re
from zlib import *

import gridtools
import credentials
import couchdb
import pickle

from picas.actors import RunActor
from picas.clients import CouchDB
from picas.iterators import TaskViewIterator
from picas.modifiers import BasicTokenModifier


# Set Main Args
view = sys.argv[1]
genome = sys.argv[2]
projectID = sys.argv[3]
soft = str("${TMPDIR}/mine_wgs_processing/job_management/joint_calling")


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
		token = {"_id": tokenname,
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


# Read Processing tokens
elif view == "DedupBQSR":

	# Skip token if exists
	tokenname = str(SM + "_DedupBQSR")
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



# Mark Duplicates tokens
elif view == "MarkDuplicates":

	# Skip token if exists
	tokenname = str(SM + "_" + build + "_MarkDuplicates")
	if tokenname in db:
		print("Skipping " + tokenname + " found in " + credentials.DBNAME)

	else:

		# Set token fields
		bam = DataFile
		bam_adler = gridtools.get_adler32_on_srm(bam)
		bamDict = {"url": bam, "adler32": bam_adler}

		# Write task script
		taskScript = """bash ${soft}/MarkDuplicates-Picard-v2.sh ${bam} ${SM}"""
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


# BQSR-GATK tokens
elif view == "BQSR_GATK":

	# Skip token if exists
	build = sys.argv[4]
	tokenname = str(SM + "_" + build + "_BQSR_GATK")
	if tokenname in db:
		print("Skipping " + tokenname + " found in " + credentials.DBNAME)

	else:

		# Set token fields
		bam = DataFile
		bam_adler = gridtools.get_adler32_on_srm(bam)
		bamDict = {"url": bam, "adler32": bam_adler}

		# Write task script
		taskScript = """bash ${soft}/BQSR-GATK.sh ${bam} ${SM} ${build}"""
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
elif view == "VariantCalling_HaplotypeCaller_V2":

	# Skip token if exists
	build = sys.argv[4]
	genome = sys.argv[5]
	tokenname = str(SM + "_" + build + "_VariantCalling_HaplotypeCaller_V2")
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


# gVCF Grouping tokens
elif view == "CombineGVCF":

	# Skip token if exists
	gvcf_mode = sys.argv[4]
	chrom = sys.argv[5]
	tokenname = str(SM + "_" + gvcf_mode + "_" + chrom + "_CombineGVCF")
	if tokenname in db:
		print("Skipping " + tokenname + " found in " + credentials.DBNAME)

	else:

		# Set token fields
		grouping = DataFile
		grouping_adler = gridtools.get_adler32_on_srm(grouping)
		groupingDict = {"url": grouping, "adler32": grouping_adler}

		# Write task script
		taskScript = """bash ${soft}/CombineGVCF.sh ${grouping} ${gvcf_mode} ${chrom}"""
		taskScript = gridtools.templater(taskScript)

		# Write token
		token = {"_id": tokenname,
			"lock": 0,
			"done": 0,
			"type": view,
			"files": groupingDict,
			"gVCF_Mode": gvcf_mode,
			"Chromosome": chrom,
			"Task_ID": SM,
			"Task_Script": taskScript
		}

		# Upload token
		print("Added Token = " + tokenname + " to Database = " + credentials.DBNAME + " under View =  " + credentials.VIEW_NAME)
		db.save(token)


# Create LASER tokens
elif view == "LASER_KG":
	
	# Skip token if exists
	site = sys.argv[4]
	tokenname = str(SM + "_" + site + "_LASER_KG")
	if tokenname in db:
		print("Skipping " + tokenname + " found in " + credentials.DBNAME)

	else:

		# Set token fields
		bam = DataFile
		bam_adler = gridtools.get_adler32_on_srm(bam)
		bamDict = {"url": bam, "adler32": bam_adler}

		# Write task script
		taskScript = """bash ${soft}/LASER-1KG.sh ${bam} ${SM} ${site}"""
		taskScript = gridtools.templater(taskScript)

		# Write token
		token = {"_id": tokenname,
			"lock": 0,
			"done": 0,
			"type": view,
			"files": bamDict,
			"Site": site,
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
		token = {"_id": tokenname,
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



# Create LASER tokens
elif view == "AWS_Egress":
	
	# Skip token if exists
	awsSource = sys.argv[4]
	tokenname = str(SM + "_" + awsSource + "_AWS_Egress")
	if tokenname in db:
		print("Skipping " + tokenname + " found in " + credentials.DBNAME)

	else:

		# Write task script
		taskScript = """bash ${soft}/egress-data.sh ${awsSource} ${SM} ${DataFile}"""
		taskScript = gridtools.templater(taskScript)

		# Write token
		token = {"_id": tokenname,
			"lock": 0,
			"done": 0,
			"type": view,
			"AWS-Source": awsSource,
			"Task_ID": SM,
			"Task_Script": taskScript
		}

		# Upload token
		print("Added Token = " + tokenname + " to Database = " + credentials.DBNAME + " under View =  " + credentials.VIEW_NAME)
		db.save(token)



# Exit
else:
	print("\nExiting no valid WGS view provided\n")
