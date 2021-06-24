#!/bin/python2.7


import os, re, sys
import hashlib
from zlib import adler32
import gfal2


# Function for local adler32:   <= Copied from gridtools
def adler32_of_file(filepath):
	"""
	Calculate adler32 checksum of filepath
	"""

	BLOCKSIZE = 1048576  # that's 1 MB

	asum = 1
	with open(filepath, 'rb') as f:
		while True:
			data = f.read(BLOCKSIZE)
			if not data:
				break
			asum = adler32(data, asum)
			if asum < 0:
				asum += 2 ** 32

	return hex(asum)[2:10].zfill(8).lower()
    

# Function for remote adler32
def remote_adler32(filepath):
	"""
	Fetch remote adler32 checksum of filepath
	"""

	ctx = gfal2.creat_context()
	result = str(ctx.checksum(filepath, "adler32"))

	return result


# Parse input args
inp = sys.argv[1]
checkType = sys.argv[2]


# Determine checksum mode
if checkType == "local":
	data = adler32_of_file(inp)

elif checkType == "remote":
	data = remote_adler32(inp)

else:
	data = "no mode detected"

# Print results
print(str(data))