#!/bin/bash


####################################################################################################
####################################################################################################

# SETUP ENVIRONMENT

####################################################################################################
####################################################################################################


# Setup the Environment for the Job
. /cvmfs/softdrive.nl/bkenna/Miniconda2/bin/job_management/start.sh
host=$(hostname)
wrk=$(pwd)
view=$1
export TMPDIR=${TMPDIR}
export PATH=/cvmfs/softdrive.nl/bkenna/Miniconda2/bin:$PATH
export softdrive=/cvmfs/softdrive.nl/bkenna/Miniconda2/bin
export PYTHONPATH=/cvmfs/softdrive.nl/bkenna/Miniconda2/lib/python2.7/site-packages/:${PYTHONPATH}
export VIEW_NAME=${view}


####################################################################################################
####################################################################################################

# CHECK ENVIRONMENT BEFORE STARTING

####################################################################################################
####################################################################################################


# Exit if <4 hours on Proxy
echo -e "\\nChecking Lifetime of VOMS Proxy is >4hr\\n" 1>&2
var=$(voms-proxy-info -all | grep "timeleft" | awk 'NR == 1 {print $3}' | sed 's/://g')

if [ ${var} -lt 040000 ]
then
	echo -e "Exiting:\\t<4 hours left on VOMS proxy" 1>&2
#	exit
fi


# Check node for HC jobs
if [ ${view} == "VariantCalling_HaplotypeCaller_V2" ]
then

	# Exit if host requires twice the CPU hrs
	if [ `echo ${host} | grep -c "wn-da"` -eq 1 ] || [ `echo ${host} | grep -c "wn-ha"` -eq 1 ]
	then

		# Log and write jdl
		echo -e "\\nExiting & submitting replacement jobs:\\t${host}\\n"
		bash ${TMPDIR}/mine_wgs_processing/job_management/createJob_Parametric.sh HaplotypeCaller-Parametric.jdl 6:0 52 8 HaplotypeCaller-Parametric /bin/bash "/cvmfs/softdrive.nl/bkenna/Miniconda2/bin/job_management/TOPMed-Alignment-GridJob.v2.sh ${view}"

		# Submit, clean up and exit
		glite-wms-job-submit -d ${USER} -o HaplotypeCaller-Parametric.txt HaplotypeCaller-Parametric.jdl
		cat HaplotypeCaller-Parametric.txt
		rm -fr *
		exit
	fi
fi



# Exit if < 200GB on Scratch
echo -e "\\n\\nChecking Avilable Space on ${host}\\n" 1>&2
freespace=`stat --format "%a*%s/1024^3" -f $TMPDIR|bc`
df -h $TMPDIR

if [ $freespace -lt 200 ]
then
	echo "\\nExiting:\\t< 200 GB free space on scratch\\n" 1>&2
	exit
fi



# Install repositories
echo -e "\\n\\nInstalling MinE_WGS_Processing Repository\\n" 1>&2
cd ${TMPDIR}
git clone https://bitbucket.org/Bren-Kenna/mine_wgs_processing.git
export PATH=${TMPDIR}/mine_wgs_processing/job_management:${PATH}
export PYTHONPATH=${TMPDIR}/mine_wgs_processing/job_management:${PYTHONPATH}
chmod -R +x mine_wgs_processing/job_management/*



####################################################################################################
####################################################################################################

# RUN PIPELINE

####################################################################################################
####################################################################################################


echo -e "\\n\\nExecuting pipeline\\n" 1>&2
python ${TMPDIR}/mine_wgs_processing/realignment/bin/PiCaS-General.py ${view}
