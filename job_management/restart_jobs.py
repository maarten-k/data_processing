#!/usr/bin/python2.7

#######################################################
#######################################################

# Import modules
import os, sys, re

import credentials
import gridtools
import gridtools_extended

viewName = sys.argv[1]
state = sys.argv[2]

#######################################################
#######################################################

# Filter tokenIDs if supplied
if len(sys.argv) == 4:
	tokens = sys.argv[3]
	print("\nLoading tokenIDs from supplied list:\t" + tokens)
	with open(tokens, 'r') as data:
		tokenIDs = [ line.rstrip('\n') for line in data ]
	data.close()
	print("\nNumber of tokens to be reset:\t" + str(len(tokenIDs)) + "\n")

else:
	# Fetch all token IDs
	print("\n\nFetching tokens from view:\t" + viewName + "\n")
	tokenIDs = gridtools_extended.getTokenIDs(viewName, state)

# Restart tokens
print("Tokens to be restarted:\t" + str(len(tokenIDs)) + "\n")
for tokenID in tokenIDs:
	gridtools_extended.resetToken(tokenID)

# Summarise the token states for view as sanity check
print("\n\nChecking token states for view:\t" + viewName + "\n")
JobSummary = gridtools_extended.tokenStates(viewName)
for i in JobSummary:
	sys.stdout.write(i)
