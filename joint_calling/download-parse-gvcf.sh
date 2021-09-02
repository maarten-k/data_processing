#!/bin/bash


# Set args
. /cvmfs/softdrive.nl/projectmine_sw/software/bin/data_processing/job-variables.sh
gvcfList=$1
tgt=$2
loci=$(basename ${tgt} | sed 's/.bed//g')
wrk=$(dirname ${gvcfList})
out=${wrk}/Parsed_gVCFs


# Setup
mkdir -p ${wrk}/In_gVCFs ${out}
cd ${wrk}/In_gVCFs
touch ${wrk}/Samples-Dropped.txt


# Parse gVCF until globus is done
Nexpected=$(ls *gz | wc -l)
ls *gz | while read gvcf
	do

	# Parse active loci
	if [ ! -f ${gvcf}.tbi ]; then ${TABIX} -p vcf -f ${gvcf}; else touch ${gvcf}.tbi; fi
	${TABIX} ${gvcf} -h -R ${tgt} | uniq | ${BGZIP} -l1 -c > ${out}/tmp.subset.g.vcf.gz
	mv ${out}/tmp.subset.g.vcf.gz ${out}/${gvcf}
	${TABIX} -p vcf -f ${out}/${gvcf}
	rm -f ${gvcf} ${gvcf}.tbi

	# Manage crap
	if [ `${TABIX} ${out}/${gvcf} -R ${tgt} |head| wc -l` -le 1 ]
	then
		rm -f ${out}/${gvcf} ${out}/${gvcf}.tbi
		echo -e "${gvcf}" >> ${wrk}/Samples-Dropped.txt
	fi
done


# Log status
cd ..
rm -fr In_gVCFs
Ndropped=$(cat ${wrk}/Samples-Dropped.txt | wc -l)
Ngvcf=$(ls ${out}/*gz | wc -l)
if [ ${Nexpected} == ${Ngvcf} ]
then
	echo -e "\\n\\nAll gVCF Successfully Downloaded, unable to parse = ${Ndropped}\\n"
else
	echo -e "\\n\\nError, ${Ngvcf} were parsed out of ${Nexpected}, unable to parse = ${Ndropped}\\n"
fi
