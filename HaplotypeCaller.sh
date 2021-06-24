#!/bin/bash


# Set needed vars & directories
. ${soft}/data_processing/job-variables.sh
dbSNP=/cvmfs/softdrive.nl/projectmine_sw/resources/Build38/hs38DH/dbsnp_146.hg38.vcf.gz
tgt=/cvmfs/softdrive.nl/projectmine_sw/resources/Build38/hs38DH/cds_100bpSplice_utr_codon_mirbase.bed


# Parse inputs
cram=$1
SM=$2
Site=$3
genome=$4
base=$(basename ${cram})
wrk=${TMPDIR}/${SM}
mkdir -p ${wrk}
cd ${wrk}
gvcfDir=$(echo -e "${bams}/Realignment/${Site}/gvcf/WGS-2" | sed 's/Tape/Disk/g')
cp ${gatk4} ${wrk}/
gatk4=${wrk}/gatk-package-4.1.4.0-local.jar


# Download data
globus-url-copy ${cram} ${wrk}/${base}
globus-url-copy ${cram}.crai ${wrk}/${base}.crai


# Sanity check download
RemoteBAM=$(python ${soft}/data_processing/adler32-check.py "${cram}" "remote")
LocalBAM=$(python ${soft}/data_processing/adler32-check.py "${TMPDIR}/${SM}/${base}" "local")

if [ "${RemoteBAM}" == "${LocalBAM}" ]
then
	echo -e "\\nAdler32 of bam successfull\\n"
else
	echo -e "\\nAdler32 of bam unsuccessfull\\nExiing"
	exit
fi


# Sanity check header
if [ `${SAMTOOLS} view -H ${wrk}/${base} | grep "SQ" | grep -c "alt"` -eq 0 ]
then
	echo -e "\\nExiting data for ${SM} is not aligned to build38\\n" 
	exit
fi


# Index if none
if [ ! -f ${wrk}/${base}.crai ] && [ ! -f ${wrk}/${base}.bai ]
then
	/usr/bin/time ${SAMTOOLS} index -@ 2 ${base}
fi


# Run haplotype caller
echo -e "\\nPerforming WXS Calling\\n"
/usr/bin/time java -Djava.io.tmpdir=${wrk} -jar ${gatk4} HaplotypeCaller -R ${ref} --dbsnp ${dbSNP} -I ${base} -O ${SM}.g.vcf.gz -ERC GVCF -L ${tgt} --native-pair-hmm-threads 2 &>> ${wrk}/${SM}.vcf.log
md5sum ${wrk}/${SM}.g.vcf.gz* > ${wrk}/${SM}.md5sum
bash ${soft}/data_processing/gVCF_Check.sh ${wrk}/${SM}.g.vcf.gz


# Shard by chromosome for grouping
echo -e "\\n${genome} Calling Done for:\\t${SM}\\nUploading gVCF\\n"
gvcfDir=$(echo -e "${bams}/Realignment/${Site}/gvcf/WXS-2" | sed 's/Tape/Disk/g')
echo -e "file://${wrk}/${SM}.g.vcf.gz ${gvcfDir}/${SM}/${SM}.g.vcf.gz\\nfile://${wrk}/${SM}.md5sum ${gvcfDir}/${SM}/${SM}.md5sum\\nfile://${wrk}/${SM}.g.vcf.gz_checks.tsv ${gvcfDir}/${SM}/${SM}_checks.tsv\\nfile://${wrk}/${SM}.vcf.log ${gvcfDir}/${SM}/${SM}.vcf.log" > ${wrk}/Transfers.txt
awk '{print $2}' ${wrk}/Transfers.txt | while read line; do uberftp -rm ${line}; done
uberftp -rm ${gvcfDir}/${SM}/${SM}.g.vcf.gz.tbi
globus-url-copy -c -cd -f ${wrk}/Transfers.txt
globus-url-copy file://${wrk}/${SM}.g.vcf.gz.tbi ${gvcfDir}/${SM}/${SM}.g.vcf.gz.tbi


# Sanity check download
RemoteGVCF=$(python ${soft}/data_processing/adler32-check.py "${gvcfDir}/${SM}/#${SM}.g.vcf.gz" "remote")
LocalGVCF=$(python ${soft}/data_processing/adler32-check.py "${wrk}/${SM}.g.vcf.gz" "local")
if [ "${RemoteGVCF}" == "${LocalGVCF}" ]
then
	echo -e "\\nAdler32 of gVCF successfull:   ${SM}.g.vcf.gz\\nClearing active directory\\n"
	echo -e "\\nChecking complete\\nProcessing complete for:   ${SM}\\n"
	cd ${TMPDIR}
	rm -fr ${SM}*

else
	echo -e "\\nAdler32 of gVCF unsuccessfull\\nExiting with active directory clearing\\n"
	cd ${TMPDIR}
	rm -fr ${SM}*
	exit
fi
