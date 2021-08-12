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
out=gsiftp://gridftp.grid.sara.nl/pnfs/grid.sara.nl/data/lsgrid/Project_MinE/Disk/User/bkenna/projects/process_gvcf/Callsets/${genome}/${ProjectID}/gVCFs
loci=$(basename ${tgt} | sed 's/.bed//g')
chrom=$(echo "${loci}" | cut -d _ -f 1)
mkdir -p ${TMPDIR}/${SM}/Extracts/${loci}
cd ${TMPDIR}/${SM}/Extracts/${loci}


# Pass if gVCF exists
if [ `globus-url-copy -list ${out}/${chrom}/${loci}/${SM}/ | grep -c "tbi"` -eq 1 ]
then
	echo -e "\\nPassing on loci ${loci} already exists"
	cd ../
	rm -fr ${loci}
	exit
fi


# Pull out supplied loci
echo -e "\\nExtracting genome loci = ${loci}\\n"
(${TABIX} -h ${gVCF} donkey; ${TABIX} -R ${tgt} ${gVCF} | sort --temporary-directory=$(pwd) -nk 2) | ${BGZIP} -c > ${SM}.g.vcf.gz
${TABIX} -p vcf -f ${SM}.g.vcf.gz


# Sanity check gvcf
if [ `${TABIX} -T ${tgt} ${SM}.g.vcf.gz | wc -l` -lt 10 ]
then
	echo -e "\\nExiting, not enough data in gVCF"
	cd ..
	rm -fr ${loci}
	exit
fi

# Push results
echo -e "\\nPushing results\\n"
echo -e "file://${TMPDIR}/${SM}/Extracts/${loci}/${SM}.g.vcf.gz ${out}/${chrom}/${loci}/${SM}/${SM}.g.vcf.gz\\nfile://${TMPDIR}/${SM}/Extracts/${loci}/${SM}.g.vcf.gz.tbi ${out}/${chrom}/${loci}/${SM}/${SM}.g.vcf.gz.tbi" >> ${TMPDIR}/${SM}/Transfers.txt

# Clean up
echo -e "\\nDone, added parsed gVCF to transfer list"
# cd ..
# rm -fr ${loci}
