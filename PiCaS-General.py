#!/usr/bin/python2.7

############################################################################
############################################################################

# Import required modules

############################################################################
############################################################################


# Import modules
import os, sys, re
import credentials
import gridtools
import gridtools_extended
import couchdb
import zlib

import subprocess
from subprocess import Popen, PIPE
import path_and_vars
import glob

import inspect
import logging
import shutil
import time
import couchdblogger
import tempfile

from picas.actors import RunActor
from picas.clients import CouchDB
from picas.iterators import TaskViewIterator
from picas.modifiers import BasicTokenModifier

from functools import wraps

_log_fmt = '%(asctime)s\t%(levelname)s\t%(lineno)d\t%(message)s'
_log_date_fmt = '%Y/%m/%d %H:%M:%S'
logger = logging.getLogger('mylogger')
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(fmt=_log_fmt, datefmt=_log_date_fmt)
ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)



# Clear folder
def cleanfolder(f):
	@wraps(f)
	def wrapper(*args, **kwds):
		default_dir=os.getcwd()
		working_dir=tempfile.mkdtemp()
		os.chdir(working_dir)
		f(*args, **kwds)
		
		os.chdir(default_dir)
		shutil.rmtree(working_dir)
	return wrapper


# Run shell commands
def runShell(cmd):
	"""
	Purpose: Run input shell command within python.
	Example Execution:
		cmd = 'tar -czf LP6005615-DNA_A02.tar.gz LP6005615-DNA_A02.log LP6005615-DNA_A02_DPcnt3.txt.gz'
		log = runShell(cmd)
	"""
	if ' ' in cmd:
		cmd = cmd.split(' ')
	try:
		proc = Popen(cmd, stdout=PIPE, stderr=PIPE)
	except OSError:
		print('\nExiting cannot perform command\n')
		#sys.exit(1)
	stdout, stderr = proc.communicate()
	return [stdout, stderr]


#############################################################################
#############################################################################

# Process tokens from view

#############################################################################
#############################################################################


# Set view to iterate voer
view = sys.argv[1]
credentials.VIEW_NAME = view
os.chdir(os.environ['TMPDIR'])


# Application to process each token
class PiCaS_General(RunActor):

	def __init__(self, iterator, modifier, executestep):
		"""
		"""
		# RunActor.__init__(iterator, modifier)
		# This is what happens in RunActor:
		self.iterator = iterator
		self.modifier = modifier
		self.executestep = executestep
		self.token = None
		self.token_rev = None
		# Make a copy of the db reference
		self.db = iterator.database.db
		self.cdblogger=None
		self.tasks_processed=0


	def execute(self, args, shell=False, logname_extention="", stdout_fh=PIPE):
		"""Helper function to more easily execute applications.
		@param args: the arguments as they need to be specified for Popen.
		@return: a tuple containing the exitcode, stdout & stderr
		"""
		if isinstance(args, list):
			logger.debug(" ".join(args))
		else:
			logger.debug(args)

		start = time.time()
		try:
			proc = Popen(args, stdout=stdout_fh, stderr=PIPE, shell=shell)
		except OSError as e:

			logger.debug(e)
			logger.debug(args)
			exit(1)
		except:
			e = sys.exc_info()[0]
			print e
			logger.error("Failed execute")
			logger.error(args)
			exit(1)

#		 proc = subprocess.Popen(args, stdout=subprocess.PIPE)
#		 lines_iterator = iter(proc.stdout.readline, b"")
#		 for line in lines_iterator:
#			 print(line) # yield line

		(stdout, stderr) = proc.communicate()
		end = time.time()
		# round duration to two floating points
		duration = int((end - start) * 100) / 100.0

		if isinstance(args, list):
			args=" ".join(args)
			
		if proc.returncode != 0:
			logger.error("exit code not 0: " +
						  str(proc.returncode) +
						   "\n#####################\n" +
							" "+args + "\n\n#####################\nstdout" 
							+ stdout + "\n\n#####################\nstderr" + stderr)
		
		
		job_report = {"command": args,
					  "exitcode": proc.returncode,
					  "stdout": stdout,
					  "stderr": stderr,
					  "duration": duration}

		f = inspect.currentframe().f_back
		functname = f.f_code.co_name
		lineno = f.f_lineno
		atachment_name = str(functname) + "_" + str(lineno) + logname_extention

		self.cdblogger.put_attachment(str(job_report), atachment_name)

		if proc.returncode != 0:
			exit(1)
		return job_report


#############################################################################
#############################################################################

# Fetch run task script from the token of the active view

#############################################################################
#############################################################################

	@cleanfolder
	def GenericWorker(self, token):

		# Set Required Variables
		tokenID = token["_id"]
		Task_ID = token["Task_ID"]
		tokenProc = token["type"]
		taskScript = token["Task_Script"]
		TMPDIR = str(os.getcwd() + "/" + tokenID)
		os.mkdir(TMPDIR)
		os.chdir(TMPDIR)

		# Display Job Details
		print("\n\nJob Summary for Token:\t" + token["_id"])
		print("Job Working Directory:\t" + TMPDIR)
		print("Task ID:\t" + Task_ID)
		print("Token Task:\t" + taskScript)
		print("Active Process:\t" + tokenProc + "\n\n")


		# Perform task & compress logging
		taskScript = str(taskScript)
		job_log = gridtools.execute(gridtools.run_as_bash(taskScript))
		# compressedLog = zlib.compress(str(job_log), -1)
		logger.debug(job_log)
		print("\n\nFinished processing token:\t" + tokenID + "\n\n")


##################################################
##################################################

# DEFINE HOW TO PROCESS THE TOKEN

##################################################
##################################################


	def prepare_env(self, *kargs, **kvargs):
		pass

	def prepare_run(self, *kargs, **kvargs):
		pass

	def process_task(self, ref, token):
		# this is where all the work gets done. Start editing here.
		self.token = token
		self.token_rev = token["_rev"]
			# remove
		logger.handlers.pop()

		# add new handler which logs to token
		self.cdblogger=couchdblogger.CouchDBLogHandler(self.db, tokenid=self.token["_id"],rev=self.token["_rev"])
	   
		logger.addHandler(self.cdblogger)
		#add logger to gridtools
		gridtools.logger=logger

		# Call funtion "self.executestep"
		method = getattr(self, self.executestep)
		method(token)

		token = self.db[ref]
		token["_rev"]= self.cdblogger.rev
		token = self.modifier.close(token)
		self.db[ref] = token

	def cleanup_run(self, *kargs, **kvargs):
		pass

	def cleanup_env(self, *kargs, **kvargs):
		pass

	def run(self, maxtime=-1):
		"""Run method of the actor, executes the application code by iterating
		over the available tokens in CouchDB.
		"""
		start = time.time()
		self.prepare_env()
		
		for token in self.iterator:			
			self.prepare_run()
			print("\n\n\n\nProcessing Token:\t" + token["_id"] + "\n")
			self.process_task(token["_id"], token)
			self.cleanup_run()
			if maxtime > 0:
				now = time.time()
				if now - start > maxtime:
					sys.exit(0)




####################################################
####################################################


# Run the method of the actor
def main():

	# Connect to couchdb, configure PiCaS and logging for tokens
	client = gridtools.CouchDB(url=credentials.URL, username=credentials.USERNAME, password=credentials.PASS, db=credentials.DBNAME)
	modifier = gridtools.BasicTokenModifier()
	logger.addHandler(couchdblogger.CouchDBLogHandler(client))


	# Process tokens from view
	print("\n\nProcessing Tokens from View:\t" + view + "\n")
	max_time = int(24 * 3600)
	iterator = TaskViewIterator(client, "todo", design_doc = view)
	actor = PiCaS_General(iterator, modifier, "GenericWorker")
	actor.run(maxtime = max_time)


####################################################
####################################################


# Run the program
if __name__ == '__main__':

	main()
