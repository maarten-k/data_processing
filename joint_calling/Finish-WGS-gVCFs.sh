#!/bin/bash


# Set vars
ProjectID=$1
gVCFs=$2
tgt=$3
TABIX=/cvmfs/softdrive.nl/projectmine_sw/software/bin/tabix
BGZIP=/cvmfs/softdrive.nl/projectmine_sw/software/bin/bgzip
ProjectDir=gsiftp://gridftp.grid.sara.nl/pnfs/grid.sara.nl/data/lsgrid/Project_MinE/Disk/User/bkenna/projects/process_gvcf/Callsets/WGS/${ProjectID}
out=${ProjectDir}/gVCFs
loci=$(basename ${tgt} | sed 's/.bed//g')
chrom=$(echo "${loci}" | cut -d _ -f 1)
wrk=${TMPDIR}/${loci}
mkdir -p ${wrk}/Extracts
cd ${wrk}


# Get gVCF list
globus-url-copy ${gVCFs} file://${wrk}/
gVCFs=$(basename ${gVCFs} | awk '{print "'${wrk}'/"$1}')


# List needed samples
touch ${wrk}/Needed.txt
globus-url-copy -list ${out}/${chrom}/${loci}/ | awk 'NR > 1' > ${wrk}/SM.txt
for i in $(sort -R ${gVCFs})
	do
	sm=$(echo $i | cut -d \| -f 1)
	gvcf=$(echo $i | cut -d \| -f 2)
	if [ -z `awk '$1 ~ /^'${sm}'$/' ${wrk}/SM.txt` ]
	then
		echo -e "${sm}|${gvcf}" >> ${wrk}/Needed.txt
	fi
done


# Parse needed samples
touch ${wrk}/Transfers.txt
cat ${wrk}/Needed.txt | while read line
	do
	sm=$(echo $line | cut -d \| -f 1)
	gvcf=$(echo $line | cut -d \| -f 2)

	# Check gVCF
	if [ `globus-url-copy -list $(dirname ${gvcf})/ | grep -c "${SM}.g.vcf.gz"` -eq 0 ]
	then
		tape_gvcf=$(echo -e "${gvcf}" | sed 's/Disk/Tape/g')
		if [ `globus-url-copy -list $(dirname ${tape_gvcf})/ | grep -c "${SM}.g.vcf.gz"` -eq 0 ]
		then
			echo -e "\\nPassing on sample = ${sm}, no tape or disk gVCF"
			continue
		else
			gvcf=$(echo -e "${gvcf}" | sed 's/Disk/Tape/g')
		fi
	fi


	# Download
	globus-url-copy -c -cd ${gvcf} file://${wrk}/Extracts/${SM}/${SM}.g.vcf.gz
	globus-url-copy -c -cd ${gvcf}.tbi file://${wrk}/Extracts/${SM}/${SM}.g.vcf.gz.tbi


	# Extract loci
	(${TABIX} -h ${wrk}/Extracts/${SM}/${SM}.g.vcf.gz donkey; ${TABIX} -R ${tgt} ${wrk}/Extracts/${SM}/${SM}.g.vcf.gz | sort --temporary-directory=${wrk}/Extracts/${SM}/ -nk 2) | ${BGZIP} -c > ${wrk}/Extracts/${SM}/tmp
	mv ${wrk}/Extracts/${SM}/tmp ${wrk}/Extracts/${SM}/${SM}.g.vcf.gz
	${TABIX} -p vcf -f ${wrk}/Extracts/${SM}/${SM}.g.vcf.gz


	# Add to transfer list
	echo -e "file://${wrk}/Extracts/${SM}/${SM}.g.vcf.gz ${out}/${chrom}/${loci}/${SM}/${SM}.g.vcf.gz\\nfile://${wrk}/Extracts/${SM}/${SM}.g.vcf.gz.tbi ${out}/${chrom}/${loci}/${SM}/${SM}.g.vcf.gz.tbi" > ${wrk}/Extracts/Transfers.txt

	# Push list every 100 gVCFs
	if [ `grep -c "gz$"` -eq 100 ]
	then
		echo -e "\\nCopying 100 gVCFs"
		globus-url-copy -c -cd -f ${wrk}/Extracts/Transfers.txt
		cd ${wrk}/Extracts
		rm -fr *
		rm -f ${wrk}/Extracts/Transfers.txt
		touch ${wrk}/Extracts/Transfers.txt
	fi
done


# Transfer list if not empty
if [ `grep -c "gz$"` -gt 0 ]
then
	globus-url-copy -c -cd -f ${wrk}/Transfers.txt
fi
cd $TMPDIR
rm -fr ${loci}

