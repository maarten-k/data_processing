#!/bin/bash


####################################################################################################
####################################################################################################

# SET PIPELINE VARIABLES

####################################################################################################
####################################################################################################


# Setup the Environment for the Job
. /cvmfs/softdrive.nl/projectmine_sw/software/bin/data_processing/job-variables.sh
host=$(hostname)
wrk=$(pwd)
view=$1
export TMPDIR=${TMPDIR}
export PATH=${soft}/bin:${soft}/software/bin/data_processing:$PATH
export PYTHONPATH=${soft}/lib/python2.7/site-packages/:${PYTHONPATH}
export VIEW_NAME=${view}


####################################################################################################
####################################################################################################

# CHECK VOMS PROXY & SCRATCH SPACE BEFORE STARTING

####################################################################################################
####################################################################################################


# Exit if <4 hours on Proxy
echo -e "\\nChecking Lifetime of VOMS Proxy is >4hr\\n"
var=$(voms-proxy-info -all | grep "timeleft" | awk 'NR == 1 {print $3}' | sed 's/://g')

if [ ${var} -lt 040000 ]
then
	echo -e "Exiting:\\t<4 hours left on VOMS proxy"
#	exit
fi


# Exit if < 200GB on Scratch
echo -e "\\n\\nChecking Avilable Space on ${host}\\n"
freespace=`stat --format "%a*%s/1024^3" -f $TMPDIR|bc`
df -h $TMPDIR

if [ $freespace -lt 200 ]
then
	echo "\\nExiting:\\t< 200 GB free space on scratch\\n"
	exit
fi


####################################################################################################
####################################################################################################

# RUN PIPELINE

####################################################################################################
####################################################################################################


echo -e "\\n\\nExecuting pipeline\\n"
python ${soft}/software/bin/data_processing/PiCaS-General.py ${view}
