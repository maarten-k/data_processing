#!/bin/bash


# Set vars
SM=$1
genome=$2
ProjectID=$3
gVCF=$4
tgt=$5
wrk=${TMPDIR}/${SM}
TABIX=/cvmfs/softdrive.nl/projectmine_sw/software/bin/tabix
BGZIP=/cvmfs/softdrive.nl/projectmine_sw/software/bin/bgzip
TREE=/cvmfs/softdrive.nl/bkenna/Miniconda2/bin/tree
out=gsiftp://gridftp.grid.sara.nl/pnfs/grid.sara.nl/data/lsgrid/Project_MinE/Disk/User/bkenna/projects/process_gvcf/Callsets/${genome}/${ProjectID}/gVCFs
loci=$(basename ${tgt} | sed 's/.bed//g')
chrom=$(echo "${loci}" | cut -d _ -f 1)
mkdir -p ${wrk}
cd ${wrk}


# Check gVCF before downloading
if [ `globus-url-copy -list $(dirname $gVCF)/ | grep -c "${SM}.g.vcf.gz.tbi"` -eq 0 ]
then
	echo -e "\\nSupplied gVCF not found, checking Tape"
	Tape_gVCF=$(echo $gVCF | sed 's/Disk/Tape/g')
	if [ `globus-url-copy -list $(dirname ${Tape_gVCF})/ | grep -c "${SM}.g.vcf.gz.tbi"` -eq 0 ]
	then
		echo -e "\\nError no Tape or Disk gVCF found for ${gVCF}"
		cd ..
		rm -fr $SM
		exit
	else
		echo -e "\\nSwapping supplied disk path for tape path"
		gVCF=$(echo $gVCF | sed 's/Disk/Tape/g')
	fi
fi


# Download gVCF
echo -e "\\nDownloading gVCF"
globus-url-copy ${gVCF} file://${wrk}/${SM}.g.vcf.gz
${TABIX} -p vcf -f ${SM}.g.vcf.gz
ls -lh ${wrk}/


# Move disk-wgs to tape
if [ `echo ${gVCF} | grep -c "Disk"` -eq 1 ] && [ `echo ${gVCF} | grep -c "WGS"` -eq 1 ]
then
	Tape_gVCF=$(echo ${gVCF} | sed 's/Disk/Tape/g')
	globus-url-copy -c -cd ${gVCF} ${Tape_gVCF}
	globus-url-copy -c -cd ${gVCF}.tbi ${Tape_gVCF}.tbi
	uberftp -rm ${gVCF}
	uberftp -rm ${gVCF}.tbi
fi

# Handle genome
if [ "${genome}" == "WXS" ]
then

	# Pull out whole exome
	echo -e "\\nExtracting exome\\n"
	( ${TABIX} -h ${SM}.g.vcf.gz donkey; ${TABIX} -R ${tgt} ${SM}.g.vcf.gz | uniq) | ${BGZIP} -c > ${SM}.${genome}.g.vcf.gz
	mv ${SM}.${genome}.g.vcf.gz ${SM}.g.vcf.gz
	${TABIX} -p vcf -f ${SM}.g.vcf.gz
	ls -lh ${wrk}/


	# Sanity check gvcf
	if [ `${TABIX} -R ${tgt} ${SM}.g.vcf.gz | wc -l` -lt 10 ]
	then
		echo -e "\\nExiting, not enough data in gVCF"
		cd ..
		rm -fr ${SM}
		exit
	fi


	# Push results
	echo -e "\\nPushing results\\n"
	uberftp -rm ${out}/${SM}/${SM}.g.vcf.gz
	uberftp -rm ${out}/${SM}/${SM}.g.vcf.gz.tbi
	globus-url-copy -c -cd file://${wrk}/${SM}.g.vcf.gz ${out}/${SM}/${SM}.g.vcf.gz
	globus-url-copy -c -cd file://${wrk}/${SM}.g.vcf.gz.tbi ${out}/${SM}/${SM}.g.vcf.gz.tbi


# Otherwise pull loci
elif [ "${genome}" == "WGS" ]
then

	# Pull out whole exome
	echo -e "\\nExtracting genome loci\\n"
	touch ${wrk}/wgs-subset.sh
	${TREE} -fi ${tgt} | grep "bed$" | sort -R | while read wgs_bed
		do
		echo -e "bash ${TMPDIR}/mine_wgs_processing/job_management/joint_calling/Setup-WGS-genoDB-gVCF.sh ${SM} ${genome} ${ProjectID} ${wrk}/${SM}.g.vcf.gz ${wgs_bed}" >> ${wrk}/wgs-subset.sh
	done

	
	# Execute tasks
	touch ${wrk}/Transfers.txt
	date > ${wrk}/wgs-subset.txt
	(parallel -j 4 < ${wrk}/wgs-subset.sh) &>> ${wrk}/wgs-subset.txt
	globus-url-copy -cd -c file://${wrk}/wgs-subset.txt ${out}/Logs/${SM}-extraction.txt
	rm -f ${wrk}/${SM}.g.vcf.gz ${wrk}/${SM}.g.vcf.gz.tbi


	# Run transfers
	globus-url-copy -concurrency 2 -c -cd -f ${TMPDIR}/${SM}/Transfers.txt
	rm -fr Extracts/

# Other wise pass
else
	echo -e "\\nError genome argument not supplied. genome = ${genome} must be WGS or WXS"
fi


# Clean up
echo -e "\\nDone, removing temporary directory"
cd ..
rm -fr ${SM}
