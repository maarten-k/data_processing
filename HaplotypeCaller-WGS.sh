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
echo -e "\\nDownloading cram for:\\t${SM}\\n"
globus-url-copy ${cram} ${wrk}/${base}
${SAMTOOLS} index -@ 2 ${wrk}/${base}


# Sanity check download
RemoteBAM=$(python ${soft}/data_processing/adler32-check.py "${cram}" "remote")
LocalBAM=$(python ${soft}/data_processing/adler32-check.py "${TMPDIR}/${SM}/${base}" "local")
if [ "${RemoteBAM}" == "${LocalBAM}" ]; then echo -e "\\nAdler32 of bam successfull\\n"; else echo -e "\\nAdler32 of bam unsuccessfull\\nExiting"; exit; fi


# Sanity check header: 	Edited for Russia
if [ `${SAMTOOLS} view -H ${wrk}/${base} | grep "SQ" | grep -c "alt"` -eq 0 ]
then
	echo -e "\\nWarning, ${SM} is not aligned to an alt-aware GRCh38\\n" 
	exit
fi


# Call variants chr1
echo -e "\\n\\nCalling chr1\\n" >> ${wrk}/${SM}.vcf.log
mkdir -p ${wrk}/chr1
cd ${wrk}/chr1
/usr/bin/time java -Djava.io.tmpdir=${wrk}/chr1 -jar ${gatk4} HaplotypeCaller -R ${ref} --dbsnp ${dbSNP} -I ${wrk}/${base} -O ${wrk}/chr1/${SM}.chr1.g.vcf.gz -L chr1 -ERC GVCF --native-pair-hmm-threads 2 &>> ${wrk}/${SM}.vcf.log


# Initialize gVCF and remove temp chrom dir
if [ -f ${wrk}/chr1/${SM}.chr1.g.vcf.gz ]
then
	zcat ${wrk}/chr1/${SM}.chr1.g.vcf.gz > ${wrk}/${SM}.g.vcf
	cd $wrk
	rm -fr chr1
else
	echo -e "\\nError during variant, exiting\\n"
	cd ..
	rm -fr ${SM}
	exit
fi


# Iteratively haplotype caller and append to gVCF
for chrom in chr{2..22} chr{X..Y}
	do

	# Call variants
	echo -e "\\n\\nCalling ${chrom}\\n" >> ${wrk}/${SM}.vcf.log
	mkdir -p ${wrk}/${chrom}
	cd ${wrk}/${chrom}
	/usr/bin/time java -Djava.io.tmpdir=${wrk}/${chrom} -jar ${gatk4} HaplotypeCaller -R ${ref} --dbsnp ${dbSNP} -I ${wrk}/${base} -O ${wrk}/${chrom}/${SM}.${chrom}.g.vcf.gz -L ${chrom} -ERC GVCF --native-pair-hmm-threads 2 &>> ${wrk}/${SM}.vcf.log

	# Append to gVCF
	zcat ${wrk}/${chrom}/${SM}.${chrom}.g.vcf.gz | grep -v "#" >> ${wrk}/${SM}.g.vcf
	cd $wrk
	rm -fr ${chrom}
done


# Compress and index
${BGZIP} -c ${wrk}/${SM}.g.vcf > ${wrk}/${SM}.g.vcf.gz
${TABIX} -p vcf -f ${wrk}/${SM}.g.vcf.gz


# Calculate md5sum and check gVCF
md5sum ${wrk}/${SM}.g.vcf.gz* > ${wrk}/${SM}.md5sum
bash ${soft}/data_processing/gVCF_Check.sh ${wrk}/${SM}.g.vcf.gz


# Write transfer list and push to dCache
gvcfTapeDir=$(echo "${gvcfDir}" | sed 's/Disk/Tape/g')
echo -e "file://${wrk}/${SM}.g.vcf.gz ${gvcfTapeDir}/${SM}/${SM}.g.vcf.gz\\nfile://${wrk}/${SM}.md5sum ${gvcfDir}/${SM}/${SM}.md5sum\\nfile://${wrk}/${SM}.g.vcf.gz_checks.tsv ${gvcfDir}/${SM}/${SM}_checks.tsv\\nfile://${wrk}/${SM}.vcf.log ${gvcfDir}/${SM}/${SM}.vcf.log" > ${wrk}/Transfers.txt
awk '{print $2}' ${wrk}/Transfers.txt | while read line; do uberftp -rm ${line}; done
uberftp -rm ${gvcfTapeDir}/${SM}/${SM}.g.vcf.gz.tbi
globus-url-copy -c -cd -f ${wrk}/Transfers.txt
globus-url-copy -c -cd file://${wrk}/${SM}.g.vcf.gz.tbi ${gvcfTapeDir}/${SM}/${SM}.g.vcf.gz.tbi


# Check adler32s
RemoteGVCF=$(python ${soft}/data_processing/adler32-check.py "${gvcfTapeDir}/${SM}/${SM}.g.vcf.gz" "remote")
LocalGVCF=$(python ${soft}/data_processing/adler32-check.py "${wrk}/${SM}.g.vcf.gz" "local")
if [ "${RemoteGVCF}" == "${LocalGVCF}" ]
then
	echo -e "\\nAdler32 of gVCF successfull:\\t${SM}.g.vcf.gz\\n"
else
	echo -e "\\nAdler32 of gVCF unsuccessfull\\nClearing temp directory and exiting\\n"
	cd ${TMPDIR}
	rm -fr ${SM}*
	exit
fi


# Clean up
echo -e "\\n\\nProcessing complete for:\\t${SM}\\n"
cd ${TMPDIR}
rm -fr ${SM}*
