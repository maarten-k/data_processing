#!/bin/bash


# Load needed variables
. ${soft}/software/bin/data_processing/job-variables.sh


# Parse inputs
bam=$1
base=$(basename ${bam})
SM=$2
Site=$3

# Setup working directory
wrk=${TMPDIR}/${SM}
mkdir -p ${wrk}
cd ${wrk}

# Update output directory
build=grch38
bamDir=${bams}/Realignment/${Site}/bam/${SM}/b38
bamDirSRM=Realignment/${Site}/bam/${SM}/b38
ref=${ref38}

# Update build 38 variables
if [ "${build}" == "grch38" ] || [ "${build}" == "b38" ] || [ "${build}" == "build38" ]
then
	ref=${b38}/hs38DH.fa
	dbSNP=${b38}/dbsnp_146.hg38.vcf.gz
	Mills=${b38}/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz
	KnownIndels=${b38}/Homo_sapiens_assembly38.known_indels.vcf.gz
	BQSR_Loci=$(echo -L chr{1..22} | sed 's/ / -L /g' | sed 's/-L -L/-L/g')
	bamDir=${bamDir}/${SM}/b38
else
	BQSR_Loci=$(echo -L {1..22} | sed 's/ / -L /g' | sed 's/-L -L/-L/g')
	bamDir=${bamDir}/${SM}
fi


#################################
#################################

# Mark Duplicates

#################################
#################################


# Download CRAM
fileType=$(basename ${bam} | awk -F "." '{print $NF}')
if [ ${fileType} == "cram" ]
then

	# Sanity check download
	echo -e "\\nDownloading & Decompressing input cram\\n"
	echo -e "${bam} file://${wrk}/${base}\\n${bam}.crai file://${wrk}/${base}.crai" > ${wrk}/${SM}-Transfer.txt
	globus-url-copy -cd -f ${wrk}/${SM}-Transfer.txt
	RemoteBAM=$(python ${soft}/data_processing/adler32-check.py "${bam}" "remote")
	LocalBAM=$(python ${soft}/data_processing/adler32-check.py "${TMPDIR}/${SM}/${base}" "local")
	if [ "${RemoteBAM}" == "${LocalBAM}" ]; then echo -e "\\nAdler32 of cram successfull\\n"; else echo -e "\\nAdler32 of cram unsuccessfull\\nExiting"; exit; fi

	# Convert to bam
	/usr/bin/time ${SAMTOOLS} view -h -@ 8 -T ${ref} ${base} -b > ${SM}.bam
	${SAMTOOLS} index -@ 8 ${SM}.bam
	base=${SM}.bam
	rm -f ${SM}*cram ${SM}*cram.crai

	# Mark duplicate reads
	echo -e "\\n\\nMarking Duplicate Reads\\n"
	/usr/bin/time java -Djava.io.tmpdir=${wrk} -jar ${PICARD} MarkDuplicates I=${base} AS=true O=${SM}.dedup.bam METRICS_FILE=${SM}.dedupMetrics.txt QUIET=true COMPRESSION_LEVEL=0 2>> ${SM}.dedup.log
	rm -f ${base}*

	# Sort results for BQSR
	/usr/bin/time ${SAMTOOLS} sort -@ 8 -o ${SM}.dedup-sorted.bam ${SM}.dedup.bam
	${SAMTOOLS} index -@ 8 ${SM}.dedup-sorted.bam
	rm -f ${SM}.dedup.bam


# Download bam for BQSR
else
	echo -e "\\nDownloading & Decompressing input cram\\n"
	echo -e "${bam} file://${wrk}/${base}\\n${bam}.bai file://${wrk}/${base}.bai" > ${wrk}/${SM}-Transfer.txt
	globus-url-copy -cd -f ${wrk}/${SM}-Transfer.txt
	RemoteBAM=$(python ${soft}/data_processing/adler32-check.py "${bam}" "remote")
	LocalBAM=$(python ${soft}/data_processing/adler32-check.py "${TMPDIR}/${SM}/${base}" "local")
	if [ "${RemoteBAM}" == "${LocalBAM}" ]; then echo -e "\\nAdler32 of cram successfull\\n"; else echo -e "\\nAdler32 of cram unsuccessfull\\nExiting"; exit; fi

fi

#################################
#################################

# BQSR-GATK

#################################
#################################


# Calculate recalibration table
echo -e "\\n\\nCalculating recalibration model\\n"
base=${SM}.dedup-sorted.bam
/usr/bin/time java -Djava.io.tmpdir=${wrk} -jar ${gatk} -T BaseRecalibrator -I ${base} -R ${ref} -o ${SM}.recal -nct 8 --downsample_to_fraction .1 ${BQSR_Loci} -knownSites ${dbSNP} -knownSites ${Mills} -knownSites ${KnownIndels} 2>> ${SM}.bqsr.log

# Apply recalibration model
echo -e "\\n\\nApplying model\\n"
/usr/bin/time java -Djava.io.tmpdir=${wrk} -jar ${gatk} -T PrintReads -I ${base} -R ${ref} -nct 8 --BQSR ${SM}.recal -o ${SM}.bqsr.bam --globalQScorePrior -1.0 --preserve_qscores_less_than 6 --static_quantized_quals 10 --static_quantized_quals 20 --static_quantized_quals 30 --disable_indel_quals 2>> ${SM}.bqsr.log
rm -f ${base}* ${SM}.recal

# Sanity sort & CRAM compress
/usr/bin/time ${SAMTOOLS} sort -@ 8 ${SM}.bqsr.bam -o ${SM}.final.bam
rm -f ${SM}.bqsr.bam*
/usr/bin/time ${SAMTOOLS} view -h -@ 8 -T ${ref} -C ${SM}.final.bam > ${SM}.final-gatk.cram
/usr/bin/time ${SAMTOOLS} index -@ 8 ${SM}.final-gatk.cram
rm -f ${SM}.final.bam*
/usr/bin/time md5sum ${SM}*final-gatk* > ${SM}.final-gatk.cram.md5sum


#################################
#################################

# Push Results

#################################
#################################


# Setup transfer list & clear previous set
echo -e "file://${wrk}/${SM}.final-gatk.cram ${bamDir}/${SM}.final-gatk.cram\\nfile://${wrk}/${SM}.final-gatk.cram.crai ${bamDir}/${SM}.final-gatk.cram.crai" > ${wrk}/Transfers.txt
echo -e "file://${wrk}/${SM}.bqsr.log ${bamDir}/${SM}.bqsr.log\\nfile://${wrk}/${SM}.final-gatk.cram.md5sum ${bamDir}/${SM}.final-gatk.cram.md5sum\\nfile://${wrk}/${SM}.dedup.log ${bamDir}/${SM}.dedup.log\\nfile://${wrk}/${SM}.dedupMetrics.txt ${bamDir}/${SM}.dedupMetrics.txt" | sed 's/Tape/Disk/g' >> ${wrk}/Transfers.txt
for i in $(awk '{print $2}' ${wrk}/Transfers.txt); do uberftp -rm ${i}; done

# Run transfer
globus-url-copy -cd -c -f ${wrk}/Transfers.txt


# Check adlers of ouput
RemoteBAM=$(python ${soft}/data_processing/adler32-check.py ${bamDir}/${SM}.final-gatk.cram "remote")
LocalBAM=$(python ${soft}/data_processing/adler32-check.py ${TMPDIR}/${SM}/${SM}.final-gatk.cram "local")
if [ "${RemoteBAM}" == "${LocalBAM}" ]; then echo -e "\\nAdler32 of BQSR successfull\\n"; else echo -e "\\nAdler32 of BQSR successfull\\nExiting"; exit; fi


# Check output data: 	>2MB
FinalCram=$(ls -l ${TMPDIR}/${SM}/${SM}.final-gatk.cram | awk '{print $5}')
SrmDirRaw=$(echo ${bam} | sed -e 's/gsiftp/srm/g' -e 's/gridftp/srm/g')
SrmDirRes=$(echo ${bamDir} | sed -e 's/gsiftp/srm/g' -e 's/gridftp/srm/g')


# Clear input if good, otherwise clear the output
if [ "${FinalCram}" -gt 2000000 ]
then
	# Remove input data
	echo -e "\\nProcessing complete, removing input data\\n"
	srmrm ${SrmDirRaw}
	srmrm ${SrmDirRaw}.crai

elif [ "${FinalCram}" -lt 2000000 ]
then
	# Remove results
	echo -e "\\nRemoving results error during processing\\n"
	for i in $(awk '{ print $2 }' ${wrk}/Transfers.txt); do uberftp -rm ${i}; done

fi


# Clear instance
cd ${TMPDIR}
rm -fr ${SM}
