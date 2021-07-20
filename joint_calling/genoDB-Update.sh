#!/bin/bash


# Set vars & Parse Args
. ${TMPDIR}/mine_wgs_processing/job_management/start.sh
TABIX=/cvmfs/softdrive.nl/projectmine_sw/software/bin/tabix
ref=${b38}/hs38DH.fa
ProjectID=$1
tgt=$2
loci=$(basename ${tgt} | sed 's/.bed//g')
chrom=$(echo ${loci} | cut -d _ -f 1)
if [ `echo $tgt | grep -c WGS` -eq 1 ]
then
	GATK_Loci_ARG=$(basename ${tgt} | sed -e 's/.bed//g' -e 's/_/\t/g' | awk '{print $1":"$2"-"$3}')
else
	GATK_Loci_ARG=${tgt}
fi
gvcfDir=$3
wrk=${TMPDIR}/${ProjectID}_${loci}
ParseGVCF_Flag=${wrk}/Parse_gVCF.done.txt
outDisk=$(echo -e "${out}" | sed 's/Tape/Disk/g')
failedStreak=0
mkdir -p ${wrk} && cd ${wrk}


# Update genoDB
counter=1
while [ ! -f ${ParseGVCF_Flag} ]
do

	# Sanity check
	sleep 1h
	counter=$((${counter}+1))
	N=$(ls ${gvcfDir}/*gz | wc -l)
	echo -e "\\n\\nImport iteration N = ${counter}, Verifying parsing for N = ${N}\\n"
	rm -f ${wrk}/${ProjectID}_${loci}.sample_map && touch ${wrk}/${ProjectID}_${loci}.sample_map
	for gvcf in $(ls ${gvcfDir}/*gz)
		do

		# Pass on malformed gVCFs
		if [ `${TABIX} ${gvcf} -R ${tgt} | wc -l` -le 1 ]
		then
			rm -f ${gvcf} ${gvcf}.tbi

		else

			# Add sample if not imported
			SM=$(basename ${gvcf} | sed 's/.g.vcf.gz//g')
			if [ `awk '$1 == "'${SM}'" {print $1}' ${wrk}/${ProjectID}_${loci}.imported.txt | wc -l` -eq 0 ]; then echo -e "${SM}\\t${gvcf}" >> ${wrk}/${ProjectID}_${loci}.sample_map; fi
		fi
	done


	# Update genoDB
	N=$(wc -l ${wrk}/${ProjectID}_${loci}.sample_map | awk '{print $1}')
	if [ ${counter} -ge 180 ]; then break; fi
	if [ ${N} -le 10 ]; then sleep 1h; continue; fi
	echo -e "\\n\\nN importing = ${N}\\n"
	rm -f ${wrk}/${ProjectID}-${loci}.current-genoDB.log
	/usr/bin/time java -Djava.io.tmpdir=${wrk} -Xmx35G -jar ${gatk4} GenomicsDBImport --genomicsdb-update-workspace-path ${wrk}/genoDB/${ProjectID}-${loci} --batch-size 100 -L ${GATK_Loci_ARG} --sample-name-map ${wrk}/${ProjectID}_${loci}.sample_map --reader-threads 4 --consolidate &>> ${wrk}/${ProjectID}-${loci}.current-genoDB.log


	# Handle failed imports
	if [ `grep -ic "error" ${wrk}/${ProjectID}-${loci}.current-genoDB.log` -ge 1 ]
	then

		# Remove genoDB
		failedStreak=$((${failedStreak}+1))
		rm -fr genoDB/

		# Rollback to previous
		globus-url-copy -cd -c ${out}/genoDB/${chrom}/${loci}/${ProjectID}-${loci}.tar.gz file://${wrk}/${ProjectID}-${loci}.tar.gz

		# Unpack genoDB
		tar -xf ${ProjectID}-${loci}.tar.gz && rm -f ${ProjectID}-${loci}.tar.gz
		bash Decompression.sh
		rm -f Decompression.sh compression.sh

		# Manage failed streak
		if [ ${failedStreak} -eq 4 ]
		then

			# Exit on 4 consecutive fails
			echo -e "\\nExiting, encountered 4 consecutive failed imports\\n"
			exit

		# Otherwise continue to next iteration
		else
			continue
		fi


	# Otherwise append log file and proceed
	else		
		cat ${wrk}/${ProjectID}-${loci}.current-genoDB.log >> ${wrk}/${ProjectID}-${loci}.genoDB.log
	fi


	# Remove the imported parsed gVCF
	echo -e "\\n\\nVerification successful, proceeding to Archival\\n\\n"
	cat ${wrk}/${ProjectID}_${loci}.sample_map >> ${wrk}/${ProjectID}_${loci}.imported.txt
	for gvcf in $(cut -f 2 ${wrk}/${ProjectID}_${loci}.sample_map)
		do
		rm -f ${gvcf} ${gvcf}.tbi
	done


	# Archive & push to dCache
	echo -e "\\n\\nArchiving genoDB for dCache upload\\n\\n"
	bash ${TMPDIR}/mine_wgs_processing/job_management/manage_genoDB.sh ${wrk} ${ProjectID}-${loci} ${wrk}/${ProjectID}-${loci}
	uberftp -rm ${out}/genoDB/${chrom}/${loci}/${ProjectID}-${loci}.tar.gz
	globus-url-copy -cd file://${wrk}/${ProjectID}-${loci}.tar.gz ${out}/genoDB/${chrom}/${loci}/${ProjectID}-${loci}.tar.gz
	rm -f ${wrk}/${ProjectID}-${loci}.tar.gz
	# rm -f ${wrk}/${ProjectID}-${loci}.genoDB.log


	# Wait for a new batch
	echo -e "\\n\\nUpdate & archive complete, awaiting next batch\\n\\n"

done



# Logging
rm -f ${wrk}/${ProjectID}_${loci}.sample_map && touch ${wrk}/${ProjectID}_${loci}.sample_map
for gvcf in $(ls ${gvcfDir}/* | grep "gz$")
	do
	if [ `${TABIX} ${gvcf} -R ${tgt} | wc -l` -le 1 ]
	then
		rm -f ${gvcf} ${gvcf}.tbi
	else
		SM=$(basename ${gvcf} | sed 's/.g.vcf.gz//g')
		if [ `awk '$1 == "'${SM}'" {print}' ${wrk}/${ProjectID}_${loci}.imported.txt | wc -l` -eq 0 ]; then echo -e "${SM}\\t${gvcf}" >> ${wrk}/${ProjectID}_${loci}.sample_map; fi
	fi
done


# Update genoDB
N=$(wc -l ${wrk}/${ProjectID}_${loci}.sample_map | awk '{print $1}')
echo -e "\\n\\nN importing = ${N}\\n"
if [ ${N} == 0 ]; then exit; fi
/usr/bin/time java -Djava.io.tmpdir=${wrk} -Xmx35G -jar ${gatk4} GenomicsDBImport --genomicsdb-update-workspace-path ${wrk}/genoDB/${ProjectID}-${loci} --batch-size 100 -L ${GATK_Loci_ARG} --sample-name-map ${wrk}/${ProjectID}_${loci}.sample_map --reader-threads 4 --consolidate &>> ${wrk}/${ProjectID}-${loci}.genoDB.log


# Remove the imported parsed gVCF
echo -e "\\n\\nVerification successful, proceeding to Archival\\n\\n"
cat ${wrk}/${ProjectID}_${loci}.sample_map >> ${wrk}/${ProjectID}_${loci}.imported.txt
for gvcf in $(cut -f 2 ${wrk}/${ProjectID}_${loci}.sample_map)
	do
	rm -f ${gvcf} ${gvcf}.tbi
done


# Archive & push to dCache
echo -e "\\n\\nArchiving genoDB for dCache upload\\n\\n"
bash ${TMPDIR}/mine_wgs_processing/job_management/manage_genoDB.sh ${wrk} ${ProjectID}-${loci} ${wrk}/${ProjectID}-${loci}
uberftp -rm ${out}/genoDB/${chrom}/${loci}/${ProjectID}-${loci}.tar.gz
globus-url-copy -cd file://${wrk}/${ProjectID}-${loci}.tar.gz ${out}/genoDB/${chrom}/${loci}/${ProjectID}-${loci}.tar.gz
rm -f ${wrk}/${ProjectID}-${loci}.tar.gz
# rm -f ${wrk}/${ProjectID}-${loci}.genoDB.log


# Log samples not imported to dCache
echo -e "\\n\\nArchiving complete, uploading list of samples not imported to ${out}/${loci}/genoDB/${ProjectID}-${loci}.ForImport.txt\\n\\n"
awk '{print $1"|"}' ${wrk}/${ProjectID}_${loci}.imported.txt | grep -vxf - ${gVCFs} > ${wrk}/${ProjectID}-${loci}.ForImport.txt
uberftp -rm ${out}/genoDB/${chrom}/${loci}/${ProjectID}-${loci}.ForImport.txt
globus-url-copy file://${wrk}/${ProjectID}-${loci}.ForImport.txt ${out}/genoDB/${chrom}/${loci}/${ProjectID}-${loci}.ForImport.txt
