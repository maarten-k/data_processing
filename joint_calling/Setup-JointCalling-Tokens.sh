#!/bin/bash


# Set vars
. /cvmfs/softdrive.nl/bkenna/Miniconda2/bin/job_management/start.sh
cd /home/bkenna/project/pipelines/mine_wgs_processing/job_management
wrk=~/projects/build38_Callset
project=srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/lsgrid/Project_MinE/Disk/User/bkenna/projects/process_gvcf/Callsets
projectGlobus=gsiftp://gridftp.grid.sara.nl/pnfs/grid.sara.nl/data/lsgrid/Project_MinE/Disk/User/bkenna/projects/process_gvcf/Callsets
bam=${project}/bam
gvcf=${project}/gvcf
db=~/project/databases/meta.db
b38=Disk/User/bkenna/projects/resources/JointCalling/WXS
mkdir -p ${wrk}/Tasks


# Set data
ProjectID=$1
mode=$2
gvcfs=$3
base=$(basename ${gvcfs})
out=${projectGlobus}/${mode}/${ProjectID}
outSRM=${project}/${mode}/${ProjectID}


# List data
# echo -e "select SM, 'gsiftp://gridftp.grid.sara.nl/pnfs/grid.sara.nl/data/lsgrid/Project_MinE/' || b38_gVCF from b38_SM where b38_gVCF is not null and Site not like '%TOPMed%' and b38_gVCF like '%${mode}-2%' and WGS_Import is null order by random() limit ${N};" | sqlite3 ${db} > ${wrk}/${ProjectID}.list
# uberftp -rm ${out}/VCF/${base}
# globus-url-copy -c -cd file://${wrk}/${ProjectID}.list ${out}/VCF/${base}


# Create Token Import Script
echo -e "cd ~/project/pipelines/mine_wgs_processing/job_management" > ${wrk}/Upload-Tokens-${ProjectID}.sh
cd /projectmine-nfs/
for tgt in $(~/tree-1.7.0/tree -fi ${b38} | grep "bed$" | awk '{print "gsiftp://gridftp.grid.sara.nl/pnfs/grid.sara.nl/data/lsgrid/Project_MinE/"$0}' | sort -R)
	do

	# Set task vars
	shard=$(basename ${tgt} | sed 's/.bed//g')
	chrom=$(dirname ${tgt} | awk 'BEGIN{FS="/"} {print $NF}')

	# Create token: 	View, gVCF-List, ID, Loci, Shard
	echo -e "/usr/bin/python create_tokens.Final.py \"JointCalling\" \"${out}/VCF/${base}\" \"${ProjectID}\" \"${tgt}\" \"${shard}\" \"${mode}\" " >> ${wrk}/Upload-Tokens-${ProjectID}.sh

done

# Print logging
wc -l ${wrk}/Upload-Tokens-${ProjectID}.sh
