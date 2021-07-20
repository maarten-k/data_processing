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
mode="WXS"
out=${projectGlobus}/${mode}/${ProjectID}
outSRM=${project}/${mode}/${ProjectID}


# List data
# echo -e "select * from df3_wes order by random();" | sqlite3 ${db} > ${wrk}/${ProjectID}.list
# cd /projectmine-nfs/Disk/User/bkenna/projects/process_gvcf/Callsets/WXS/${ProjectID}/
# ~/tree-1.7.0/tree -fish gVCFs/ | grep "gz$" > ${wrk}/${ProjectID}-gVCFs.txt
# grep "[0-9][0-9][0-9]M" ${wrk}/${ProjectID}-gVCFs.txt | awk '{print $2}' | awk -F "/" '{print $(NF-1)"|gsiftp://gridftp.grid.sara.nl/pnfs/grid.sara.nl/data/lsgrid/Project_MinE/Disk/User/bkenna/projects//process_gvcf/Callsets/WXS/DF3_WES/"$0}' >> ${wrk}/${ProjectID}.list
# echo -e "select b38_SM.SM, 'gsiftp://gridftp.grid.sara.nl/pnfs/grid.sara.nl/data/lsgrid/Project_MinE/' || b38_gVCF from b38_SM left join df3_wes using(SM) where b38_gVCF is not null and b38_gVCF like '%WXS%' and SM like 'SRR%' and WXS_Import is null and df3_wes.SM is null order by random() limit 4464;" | sqlite3 ${db} | grep "Disk" >> ${wrk}/${ProjectID}.list
# sort ${wrk}/${ProjectID}.list | uniq > tmp
# mv tmp ${wrk}/${ProjectID}.list
# uberftp -rm ${out}/VCF/${ProjectID}.list
# globus-url-copy -c -cd file://${wrk}/${ProjectID}.list ${out}/VCF/${ProjectID}.list


# Create Token Import Script
echo -e "cd ~/project/pipelines/mine_wgs_processing/job_management" > ${wrk}/Upload-Tokens-${ProjectID}.sh
cd /projectmine-nfs/
for tgt in $(~/tree-1.7.0/tree -fi ${b38} | grep "bed$" | awk '{print "gsiftp://gridftp.grid.sara.nl/pnfs/grid.sara.nl/data/lsgrid/Project_MinE/"$0}' | sort -R)
	do

	# Set task vars
	shard=$(basename ${tgt} | sed 's/.bed//g' | sed 's/.shard/_shard/g')
	chrom=$(dirname ${tgt} | awk 'BEGIN{FS="/"} {print $NF}')

	# Create token: 	View, gVCF-List, ID, Loci, Shard
	echo -e "/usr/bin/python create_tokens.Final.py \"JointCalling\" \"${out}/VCF/${ProjectID}.list\" \"${ProjectID}\" \"${tgt}\" \"${shard}\" \"${mode}\" " >> ${wrk}/Upload-Tokens-${ProjectID}.sh

done

# Print logging
wc -l ${wrk}/Upload-Tokens-${ProjectID}.sh
