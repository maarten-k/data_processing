#!/bin/bash


# Load needed variables
. ${soft}/data_processing/job-variables.sh


# Exit if not enough disk space
diskSpace=$(curl -s 'https://ganglia.surfsara.nl/graph.php?g=dcache_poolgroup_report&poolgroup=projectmine_writediskpools&z=xxlarge&c=Tier1%20Cluster&h=m-dcmain.grid.sara.nl&r=hour&csv' | grep -v "NaN" | tail -n 1 | awk 'BEGIN{FS=","} { print ($NF - $(NF-2)) / 1000000000000}' | awk '{round=sprintf("%d", $1+0.5) } {print round}' | bc)
if [ ${diskSpace} -lt 15 ] || [ -z "${diskSpace}" ]
then
	echo -e "\\nError not enough disk space:\\t${diskSpace}TB\\n"
	# exit
else
	echo -e "\\nProceeding to processing with viable disk space:\\t${diskSpace}TB\\n"
fi


# Parse inputs
inp=$1
SM=$2
Site=$3
build=${4:-hg19}


# Setup working directory
wrk=${TMPDIR}/${SM}
mkdir -p ${TMPDIR}/${SM}
cd ${TMPDIR}/${SM}


# Set reference genome
if [ "${build}" == "grch37" ]
then

        # Set GRCh37
        echo -e "\\n\\nPerforming Fastq Extraction on GRCh37\\n"
        ref=${ref}

elif [ "${build}" == "hg19" ]
then

        # Extract Fastq from build37-HG19
        echo -e "\\n\\nPerforming Fastq Extraction on hg19\\n"
        ref=${refHG19}

elif [ "${build}" == "b38" ] || [ "${build}" == "grch38" ]
then

        # Extract Fastq from build38
        echo -e "\\n\\nPerforming Fastq Extraction on b38\\n"
        ref=${ref38}

elif [ "${build}" == "deCode" ] || [ "${build}" == "decode" ]
then
        # Extract Fastq from deCode-build38
        echo -e "\\n\\nPerforming Fastq Extraction on deCode-GRCh38\\n"
        ref=/cvmfs/softdrive.nl/projectmine_sw/resources/Build38/deCode-Reference/genome.fa

else
	# Otherwise exit
        echo -e "\\n\\nExiting, cannot determine build"
        exit
fi



# Download & sanity check input data
base=$(basename ${inp})
globus-url-copy ${inp} ${TMPDIR}/${SM}/
RemoteBAM=$(python ${soft}/data_processing/adler32-check.py "${inp}" "remote")
LocalBAM=$(python ${soft}/data_processing/adler32-check.py "${TMPDIR}/${SM}/${base}" "local")
if [ "${RemoteBAM}" == "${LocalBAM}" ]; then echo -e "\\nAdler32 of bam successfull\\n"; else echo -e "\\nAdler32 of bam unsuccessfull\\nExiting\\n"; exit; fi


# Index & extract fastq
echo -e "\\n\\nPerforming FASTQ extraction\\n"
/usr/bin/time ${SAMTOOLS} index -@ 8 ${base}
/usr/bin/time ${SAMTOOLS} bamshuf -@ 8 --reference ${ref} --output-fmt BAM -uOn 128 ${base} ${SM}.tmp | ${SAMTOOLS} bam2fq -@ 8 -t -s /dev/null -1 ${SM}.R1.fq.gz -2 ${SM}.R2.fq.gz - > /dev/null
rm -f ${base}


# Align reads
echo -e "\\n\\nPerforming alignment to build 38\\n"
build=grch38
alignmentDir=${bams}/Realignment/${Site}/bam/${SM}/b38

/usr/bin/time ${BWAb38} mem -K 100000000 -t 8 -Y ${ref38} -R "@RG\tID:${SM}\tLB:${SM}\tSM:${SM}\tPL:ILLUMINA" ${SM}.R1.fq.gz ${SM}.R2.fq.gz 2>> ${SM}.log | ${SAMBLASTER} -a --addMateTags | ${SAMTOOLS} view -h --threads 8 -bS > ${SM}.aln.bam
rm -f ${SM}.R1.fq.gz ${SM}.R2.fq.gz
/usr/bin/time ${SAMTOOLS} sort -@ 8 ${SM}.aln.bam > ${SM}.sorted.bam
rm -f ${SM}.aln.bam
/usr/bin/time ${SAMTOOLS} view -@ 8 -h -T ${ref38} -C ${SM}.sorted.bam > ${SM}.sorted.cram
rm -f ${SM}.sorted.bam
/usr/bin/time ${SAMTOOLS} index ${SM}.sorted.cram
/usr/bin/time md5sum ${SM}.sorted.cram* > ${SM}.sorted.cram.md5sum


# Set vars for push
AlnBam=$(ls -l ${TMPDIR}/${SM}/${SM}.sorted.cram | awk '{print $5}')
SrmDirRes=$(echo ${alignmentDir} | sed -e 's/gsiftp/srm/g' -e 's/gridftp/srm/g')
inp=$(echo "${AlnBam}" | awk '{print $1/1000000000}')

# Update diskSpace
diskSpace=$(curl -s 'https://ganglia.surfsara.nl/graph.php?g=dcache_poolgroup_report&poolgroup=projectmine_writediskpools&z=xxlarge&c=Tier1%20Cluster&h=m-dcmain.grid.sara.nl&r=hour&csv' | grep -v "NaN" | tail -n 1 | awk 'BEGIN{FS=","} { print ($NF - $(NF-2)) / 1000000000000}' | awk '{round=sprintf("%d", $1+0.5) } {print round}' | bc)


# Push to dCache if results are good
if [ ${AlnBam} -gt 2000000 ]
then

	# Update dest to disk if >11TB
	echo -e "\\n\\nAlignment & Sorting complete\\nPushing results:\\t${inp}GB\\n"
	if [ ${diskSpace} -gt 30 ] || [ -z "${diskSpace}" ]; then echo -e "\\nRetaining Disk Push:\\t${diskSpace}TB\\n"; alignmentDir=$(echo -e "${alignmentDir}" | sed 's/Tape/Disk/g'); else echo -e "\\nFalling back to tape push\\n"; fi

	# Upload results
	echo -e "file://${TMPDIR}/${SM}/${SM}.sorted.cram ${alignmentDir}/${SM}.sorted.cram\\nfile://${TMPDIR}/${SM}/${SM}.sorted.cram.crai ${alignmentDir}/${SM}.sorted.cram.crai" > ${wrk}/Transfers.txt
	echo -e "file://${TMPDIR}/${SM}/${SM}.log ${alignmentDir}/${SM}.log\\nfile://${TMPDIR}/${SM}/${SM}.sorted.cram.md5sum ${alignmentDir}/${SM}.sorted.cram.md5sum" | sed 's/Tape/Disk/g' >> ${TMPDIR}/${SM}/Transfers.txt
	globus-url-copy -c -cd -f ${TMPDIR}/${SM}/Transfers.txt

	# Check adlers
	RemoteBAM=$(python ${soft}/data_processing/adler32-check.py ${alignmentDir}/${SM}.sorted.cram "remote")
	LocalBAM=$(python ${soft}/data_processing/adler32-check.py ${TMPDIR}/${SM}/${SM}.sorted.cram "local")
	if [ "${RemoteBAM}" == "${LocalBAM}" ]; then echo -e "\\nAdler32 of alignment successfull\\n"; else echo -e "\\nAdler32 of realignment unsuccessfull\\nExiting\\n"; exit; fi

	# Create dedupping token
	# python ${soft}/data_processing/create_tokens.py "DedupBQSR" "${alignmentDir}/${SM}.sorted.cram" "${SM}"
	echo -e "\\n\\nProcessing successfully completed\\nClearing instance\\n"

# Otherwise pass
else
	echo -e "\\n\\nError during processing, not pushing results <2MB:\\t${inp}GB\\n"
	echo -e "\\nClearing instance\\n"
fi


# Clear instance
cd ${TMPDIR}
rm -fr ${SM}
