#!/bin/bash

set -x
# Set vars & Parse Args
. /cvmfs/softdrive.nl/projectmine_sw/software/bin/data_processing/job-variables.sh
ref=${b38}/hs38DH.fa
ProjectID=$1
tgt=$2
batch=$3
loci=$(basename ${tgt} | sed 's/.bed//g')
chrom=$(echo ${loci} | cut -d _ -f 1)
if [ `echo $tgt | grep -c WGS` -eq 1 ]
then
	GATK_Loci_ARG=$(basename ${tgt} | sed -e 's/.bed//g' -e 's/_/\t/g' | awk '{print $1":"$2"-"$3}')
else
	GATK_Loci_ARG=$((head -n 1 ${tgt} | cut -f 1-2; tail -n 1 ${tgt} | cut -f 3) | xargs | awk '{print $1":"$2"-"$3}')
fi
gvcfDir=$3
wrk=${TMPDIR}/${ProjectID}_${loci}
mkdir -p ${wrk} && cd ${wrk}


#####################################
#####################################
# 
# Import Parsed gVCFs
# 
#####################################
#####################################


# Sanity check data and only append good to sample_map
rm -f ${wrk}/${ProjectID}_${loci}.sample_map
touch ${wrk}/${ProjectID}_${loci}.sample_map
set +x
for gvcf in $(ls ${gvcfDir}/*gz)
	do
	SM=$(basename ${gvcf} | sed 's/.g.vcf.gz//g')
	touch ${gvcf}.tbi
	if [ `${TABIX} ${gvcf} -R ${tgt} | wc -l` -le 1 ] || [ `awk '$1 ~ /^'${SM}'$/' ${wrk}/${ProjectID}_${loci}.imported.txt | wc -l` -ge 1 ]
	then
		rm -f ${gvcf} ${gvcf}.tbi
	else
		echo -e "${SM}\\t${gvcf}" >> ${wrk}/${ProjectID}_${loci}.sample_map
	fi
done
set -x

# Generate initial genoDB
N=$(wc -l ${wrk}/${ProjectID}_${loci}.sample_map | awk '{print $1}')
if [ ${N} -eq 0 ]; then exit; fi
echo -e "\\n\\nN importing = ${N}\\n"
if [ ! -d ${wrk}/genoDB/${ProjectID}-${loci} ]
then

	# Create workspace
	/usr/bin/time -f 'timiming: %C "%E real,%U user,%S sys CPU Percentage: %P maxres: %M' java -Djava.io.tmpdir=${wrk} -Xmx35G -jar ${gatk4} GenomicsDBImport --genomicsdb-workspace-path ${wrk}/genoDB/${ProjectID}-${loci} --batch-size 250 -L ${GATK_Loci_ARG} --sample-name-map ${wrk}/${ProjectID}_${loci}.sample_map --reader-threads 2 --consolidate &>> ${wrk}/${ProjectID}-${loci}.current-genoDB.log

else

	# Otherwise update
	/usr/bin/time -f 'timiming: %C "%E real,%U user,%S sys CPU Percentage: %P maxres: %M' -Djava.io.tmpdir=${wrk} -Xmx35G -jar ${gatk4} GenomicsDBImport --genomicsdb-update-workspace-path ${wrk}/genoDB/${ProjectID}-${loci} --batch-size 250 -L ${GATK_Loci_ARG} --sample-name-map ${wrk}/${ProjectID}_${loci}.sample_map --reader-threads 2 --consolidate &>> ${wrk}/${ProjectID}-${loci}.current-genoDB.log
fi


# Locally clear imported gVCF
cat ${wrk}/${ProjectID}_${loci}.sample_map >> ${wrk}/${ProjectID}_${loci}.imported.txt
set +x
for gvcf in $(cut -f 2 ${wrk}/${ProjectID}_${loci}.sample_map)
	do
	rm -f ${gvcf} ${gvcf}.tbi
done
set -x


# Remotely clear the Pre-parsed gVCF only
for gvcf in $(grep "Callsets/WGS" ${batch} | awk '{print $1}' )
	do
	uberftp -rm ${gvcf}
	uberftp -rm ${gvcf}.tbi
	uberftp -rmdir $(dirname ${gvcf})
done


#####################################
#####################################
# 
# Manage genoDB
# 
#####################################
#####################################


# Handle failed imports
if [ `grep -ic "error" ${wrk}/${ProjectID}-${loci}.current-genoDB.log` -ge 1 ]
then

	# Rollback to previous genoDB
	rm -fr genoDB/
	globus-url-copy -cd -c ${out}/genoDB/${chrom}/${loci}/${ProjectID}-${loci}.tar.gz file://${wrk}/${ProjectID}-${loci}.tar.gz

	# Unpack the previous genoDB
	tar -xf ${ProjectID}-${loci}.tar.gz && rm -f ${ProjectID}-${loci}.tar.gz
	bash Decompression.sh
	rm -f Decompression.sh compression.sh ${wrk}/${ProjectID}-${loci}.current-genoDB.log

# Otherwise append log file
else
	cat ${wrk}/${ProjectID}-${loci}.current-genoDB.log >> ${wrk}/${ProjectID}-${loci}.genoDB.log
	rm -f ${wrk}/${ProjectID}-${loci}.current-genoDB.log
fi



#####################################
#####################################
# 
# Sanity Check genoDB
# 
#####################################
#####################################


# Check genoDB
testLoci=$(head -n 1 ${tgt} | awk '{print $1":"$2"-"$2+100}')
java -Djava.io.tmpdir=${wrk} -jar ${gatk4} GenotypeGVCFs -R ${ref} -O ${wrk}/${ProjectID}-${loci}.vcf.gz -D ${dbSNP38} -G StandardAnnotation -G AS_StandardAnnotation -V gendb://${wrk}/genoDB/${ProjectID}-${loci} -L ${testLoci} &>> ${wrk}/${ProjectID}-${loci}.Joint-Calling.log
tabix -h ${wrk}/${ProjectID}-${loci}.vcf.gz donkey | tail -n 1 | cut -f 10- | sed 's/\t/\n/g' > SM.txt
# sort ${wrk}/${ProjectID}_${loci}.imported.txt | uniq > tmp
# mv tmp ${wrk}/${ProjectID}_${loci}.imported.txt
# for sm in $(cut -f 1 SM.txt); do awk '$1 ~ /^'${sm}'$/' ${wrk}/${ProjectID}_${loci}.imported.txt; done > tmp
# mv tmp ${wrk}/${ProjectID}_${loci}.imported.txt


# Print metrics
genoDB_size=$(du -sh genoDB/* | awk '{print $1}')
genoDB_verified=$(if [ -f ${wrk}/${ProjectID}-${loci}.vcf.gz.tbi ]; then echo "1"; else echo "0"; fi)
N_expected=$(cat ${ProjectID}.list | wc -l)
N_imported=$(cat SM.txt | wc -l)
Sample_MD5=$(cat SM.txt | md5sum - | awk '{print $1}')
echo -e "${ProjectID}-${loci}\\t${chrom}\\t${loci}\\t${genoDB_size}\\t${genoDB_verified}\\t${N_expected}\\t${N_imported}\\t${Sample_MD5}" > genoDB-data.txt
echo -e "file://${wrk}/SM.txt ${outDisk}/Checks/${chrom}/${loci}/SM.txt\\nfile://${wrk}/genoDB-data.txt ${outDisk}/Checks/${chrom}/${loci}/genoDB-data.txt\\nfile://${wrk}/${ProjectID}_${loci}.imported.txt ${outDisk}/Logs/genoDB/${chrom}/${loci}/${ProjectID}_${loci}.imported.txt\\nfile://${wrk}/${ProjectID}-${loci}.genoDB.log ${outDisk}/Logs/genoDB/${chrom}/${loci}/${ProjectID}-${loci}.genoDB.log\\nfile://${wrk}/${ProjectID}-${loci}.Joint-Calling.log ${outDisk}/Checks/${chrom}/${loci}/${ProjectID}-${loci}.Joint-Calling.log" > Transfers.txt
awk '{print $2}' Transfers.txt | while read line; do uberftp -rm ${line}; done
globus-url-copy -c -cd -f ${wrk}/Transfers.txt
rm -f ${wrk}/${ProjectID}-${loci}.vcf.gz ${wrk}/${ProjectID}-${loci}.vcf.gz.tbi ${wrk}/${ProjectID}_${loci}.sample_map


#####################################
#####################################
# 
# Compress Archive DB to dCache
# 
#####################################
#####################################


# Archive & push to dCache
echo -e "\\n\\nArchiving genoDB for dCache upload\\n\\n"
/usr/bin/time -f 'timiming: %C "%E real,%U user,%S sys CPU Percentage: %P' bash ${soft}/software/bin/data_processing/joint_calling/manage_genoDB.sh ${wrk} ${ProjectID}-${loci} ${wrk}/${ProjectID}-${loci}
uberftp -rm ${out}/genoDB/${chrom}/${loci}/${ProjectID}-${loci}.tar.gz
globus-url-copy -cd file://${wrk}/${ProjectID}-${loci}.tar.gz ${out}/genoDB/${chrom}/${loci}/${ProjectID}-${loci}.tar.gz
rm -f ${wrk}/${ProjectID}-${loci}.tar.gz
