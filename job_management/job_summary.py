#!/usr/bin/python2.7

import os, sys, re
import credentials
import gridtools
import gridtools_extended

viewName = sys.argv[1]
out = sys.argv[2]


# Count state of job logs
print("\nSummarising Token States for Job:\t" + viewName + "\n")
states = gridtools_extended.tokenStates(viewName)
for i in states:
	sys.stdout.write(i)


# Fetch done and locked
if len(out) >= 1:

	# Fetch logs and attachments for completed jobs
	print("\n\nProcessing done tokens:")
	DoneOut = str(out + '/' + viewName + '_done')
	try:
		os.mkdir(DoneOut)
	except OSError:
		print("Output directory exists")
	tokenIDs = gridtools_extended.getTokenIDs(viewName, 'done')
	done = len(tokenIDs)
	if done > 0:
		print("\nFetching done tokens N = " + str(done))
		for tokenID in tokenIDs:
			token, tokenLogFile = gridtools_extended.tokenLogs(viewName, tokenID, 'done')
			gridtools_extended.writeFile(token, str(DoneOut + '/' + tokenID + '.token'))
			gridtools_extended.writeFile(tokenLogFile, str(DoneOut + '/' + tokenID + '.tokenLog'))
			gridtools_extended.attachmentFromToken(viewName, tokenID, str(DoneOut + '/' + tokenID + '.attachment'))
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
	tokenIDs = gridtools_extended.getTokenIDs(viewName, 'locked')
	locked = len(tokenIDs)

	if locked > 0:
		print("\nFetching locked tokens N = " + str(locked))
		for tokenID in tokenIDs:
			token, tokenLogFile = gridtools_extended.tokenLogs(viewName, tokenID, 'locked')
			gridtools_extended.writeFile(token, str(LockedOut + '/' + tokenID + '.token'))
			gridtools_extended.writeFile(tokenLogFile, str(LockedOut + '/' + tokenID + '.tokenLog'))
			gridtools_extended.attachmentFromToken(viewName, tokenID, str(LockedOut + '/' + tokenID + '.attachment'))
		print("Logs for done token saved to:\t" + LockedOut)
	else:
		print("\nNo locked tokens to fetch")
