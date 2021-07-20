#!/bin/bash


# Set vars
RCLONE=~/rclone-v1.54.0-linux-amd64/rclone
TREE=~/tree-1.7.0/tree
BGZIP=/hpc/hers_en/bkenna/software/miniconda3/bin/bgzip
gatk4=/hpc/hers_en/bkenna/software/gatk-4.1.4.0/gatk-package-4.1.4.0-local.jar
ref=/hpc/hers_en/bkenna/software/reference/hs38DH.fa
dbSNP=/hpc/hers_en/bkenna/software/reference/dbsnp_146.hg38.vcf.gz
module load tabix/0.2.6 Java/1.8.0_60
dataDir=/hpc/hers_en/bkenna/callsets
ProjectID=$1
tgt=$2
copyMode=${3:-0}


# Set task specific vars
loci=$(basename ${tgt} | sed 's/.bed//g' | sed 's/.shard/_shard/g')
chrom=$(echo ${loci} | cut -d _ -f 1)
GATK_Loci_ARG=$((head -n 1 ${tgt} | cut -f 1-2; tail -n 1 ${tgt} | cut -f 3) | xargs | awk '{print $1":"$2"-"$3}')
wrk=${dataDir}/${ProjectID}/${chrom}/${loci}


# Create targets for mounts & task directories
mkdir -p ${wrk}/genoDB ${wrk}/Logs ${wrk}/VCF ${wrk}/Parsed_gVCFs ${wrk}/In_gVCFs


#################################
#################################
# 
# Get and Parse gVCF
# 
#################################
#################################


# Copy data & unpack genoDB
cd ${wrk}
if [ ${copyMode} -eq 0 ]
then
	${RCLONE} --config=${dataDir}/scripts/joint_calling.conf copy genodb_output:VCF/${ProjectID}.list ./
	${RCLONE} --config=${dataDir}/scripts/joint_calling.conf copy genodb_input:genoDB/${chrom}/${loci}/${ProjectID}-${loci}.tar.gz ./
else
	scp bkenna@mine-ui.grid.sara.nl:/projectmine-nfs/Tape/User/bkenna/projects/process_gvcf/Callsets/WXS/DF3_WES/genoDB/${chrom}/${loci}/${ProjectID}-${loci}.tar.gz ./
fi

tar -xf ${ProjectID}-${loci}.tar.gz && rm -f ${ProjectID}-${loci}.tar.gz
bash Decompression.sh


# Filter imported samples from the gVCF list
sed 's/,/\n/g' genoDB/${ProjectID}-${loci}/callset.json | grep -wo "sample.*" | cut -d \: -f 2 | cut -d \" -f 2 | awk '{ print $1"\t'${wrk}'/"$1".g.vcf.gz"}' > ${wrk}/${ProjectID}_${loci}.imported.txt

rm -f ${ProjectID}-toImport.list
touch ${ProjectID}-toImport.list
awk -F "/" '{print "/"$NF}' ${dataDir}/scripts/umass-cirulli.txt | while read gvcf
	do
	if [ -z `grep "${gvcf}$" ${wrk}/${ProjectID}_${loci}.imported.txt | awk 'NR == 1 {print $1}'` ]
	then
		grep "${gvcf}" ${dataDir}/scripts/umass-cirulli.txt >> ${ProjectID}-toImport.list
	fi
done


# Exit if no samples
if [ `cat ${ProjectID}-toImport.list | wc -l` -eq 0 ]
then
	echo -e "\\nExiting no samples to import"
	rm -fr genoDB/
	exit
fi


# Setup download lists
rm -f ${wrk}/download_gvcf.sh
touch ${wrk}/download_gvcf.sh
for i in $(sort -R ${ProjectID}-toImport.list)
	do
	sm=$(echo -e "${i}" | cut -d \/ -f 1)
	gvcf=$(echo -e "${i}" | cut -d \| -f 2)
	if [ -z `echo $gvcf | grep "process_gvcf"` ]
	then
		echo -e "${gvcf}" | cut -d \/ -f 14- | awk '{ print "'${RCLONE}' --config='${dataDir}'/scripts/joint_calling.conf copy gvcf_dir:"$1" '${wrk}'/In_gVCFs/" }' >> ${wrk}/download_gvcf.sh
		echo -e "${gvcf}.tbi" | cut -d \/ -f 14- | awk '{ print "'${RCLONE}' --config='${dataDir}'/scripts/joint_calling.conf copy gvcf_dir:"$1" '${wrk}'/In_gVCFs/" }' >> ${wrk}/download_gvcf.sh
	else
		echo -e "${gvcf}" | cut -d \/ -f 18- | awk '{ print "'${RCLONE}' --config='${dataDir}'/scripts/joint_calling.conf copy genodb_output:"$1" '${wrk}'/In_gVCFs/" }' >> ${wrk}/download_gvcf.sh
		echo -e "${gvcf}.tbi" | cut -d \/ -f 18- | awk '{ print "'${RCLONE}' --config='${dataDir}'/scripts/joint_calling.conf copy genodb_output:"$1" '${wrk}'/In_gVCFs/" }' >> ${wrk}/download_gvcf.sh
	fi
done

echo -e "\\nDownloading gVCFs\\n"
bash ${wrk}/download_gvcf.sh &>> /dev/null


# Parse gVCFs
rm ${wrk}/Samples-Dropped.txt
touch ${wrk}/Samples-Dropped.txt
for gvcf in $(ls ${wrk}/In_gVCFs/*gz)
	do

	# Store sample ID
	sm=$(echo "${gvcf}" | awk -F "/" '{print $NF}' | sed 's/.g.vcf.gz//g')

	# Pull out loci
	if [ ! -f ${gvcf}.tbi ]; then rm -f ${gvcf}; echo -e "${wrk}/Parsed_gVCFs/${sm}.g.vcf.gz" >> ${wrk}/Samples-Dropped.txt; continue;else touch ${gvcf}.tbi; fi
	tabix ${gvcf} -h -B ${tgt} | uniq | ${BGZIP} -c > ${wrk}/Parsed_gVCFs/${sm}.g.vcf.gz
	tabix -p vcf -f ${wrk}/Parsed_gVCFs/${sm}.g.vcf.gz

	# Manage crap
	if [ `tabix ${wrk}/Parsed_gVCFs/${sm}.g.vcf.gz -B ${tgt} | wc -l` -le 1 ]
	then
		rm -f ${wrk}/Parsed_gVCFs/${sm}.g.vcf.gz ${wrk}/Parsed_gVCFs/${sm}.g.vcf.gz.tbi
		echo -e "${wrk}/Parsed_gVCFs/${sm}.g.vcf.gz" >> ${wrk}/Samples-Dropped.txt
	fi
done


# Stop if all required samples are crap
if [ `ls ${wrk}/Parsed_gVCFs/ | grep -c "gz$"` -eq 0 ]
then
	echo -e "\\nExiting no samples to import\\n"
	exit
fi


#################
# 
# Import gVCF
# 
#################


# Sanity check data and only append good to sample_map
rm -f ${wrk}/${ProjectID}_${loci}.sample_map
touch ${wrk}/${ProjectID}_${loci}.sample_map
for gvcf in $(ls ${wrk}/Parsed_gVCFs/*gz)
	do
	SM=$(basename ${gvcf} | sed 's/.g.vcf.gz//g')
	touch ${gvcf}.tbi
	if [ `tabix ${gvcf} -B ${tgt} | wc -l` -le 1 ] || [ `awk '$1 ~ /^'${SM}'$/' ${wrk}/${ProjectID}_${loci}.imported.txt | wc -l` -ge 1 ]
	then
		rm -f ${gvcf} ${gvcf}.tbi
	else
		echo -e "${SM}\\t${gvcf}" >> ${wrk}/${ProjectID}_${loci}.sample_map
	fi
done


# Otherwise update
rm -f ${wrk}/${ProjectID}-${loci}.current-genoDB.log
java -Djava.io.tmpdir=${wrk} -Xmx35G -jar ${gatk4} GenomicsDBImport --genomicsdb-update-workspace-path ${wrk}/genoDB/${ProjectID}-${loci} --batch-size 250 -L ${GATK_Loci_ARG} --sample-name-map ${wrk}/${ProjectID}_${loci}.sample_map --reader-threads 2 --consolidate &>> ${wrk}/${ProjectID}-${loci}.current-genoDB.log


# Locally clear imported gVCF
cat ${wrk}/${ProjectID}_${loci}.sample_map >> ${wrk}/${ProjectID}_${loci}.imported.txt
for gvcf in $(cut -f 2 ${wrk}/${ProjectID}_${loci}.sample_map)
	do
	rm -f ${gvcf} ${gvcf}.tbi
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

	# Clear loci directory
	echo -e "\\nExiting, error on importing current batch\\n"
	cd ${wrk}
	cd ../
	# rm -fr ${loci}

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
echo -e "\\nSanity Checking genoDB\\n"
testLoci=$(head -n 1 ${tgt} | awk '{print $1":"$2"-"$2+50}')
java -Djava.io.tmpdir=${wrk} -jar ${gatk4} GenotypeGVCFs -R ${ref} -O ${wrk}/${ProjectID}-${loci}.vcf.gz -D ${dbSNP} -G StandardAnnotation -G AS_StandardAnnotation -V gendb://${wrk}/genoDB/${ProjectID}-${loci} -L ${testLoci} &>> ${wrk}/${ProjectID}-${loci}.Joint-Calling.log


# Print metrics
tabix ${wrk}/${ProjectID}-${loci}.vcf.gz -h donkey | tail -n 1 | cut -f 10- | sed 's/\t/\n/g' > SM.txt
genoDB_size=$(du -sh genoDB/* | awk '{print $1}')
genoDB_verified=$(if [ -f ${wrk}/${ProjectID}-${loci}.vcf.gz.tbi ]; then echo "1"; else echo "0"; fi)
N_expected=$(cat ${ProjectID}.list | wc -l)
N_imported=$(cat SM.txt | wc -l)
Sample_MD5=$(cat SM.txt | md5sum - | awk '{print $1}')
echo -e "${ProjectID}-${loci}\\t${chrom}\\t${loci}\\t${genoDB_size}\\t${genoDB_verified}\\t${N_expected}\\t${N_imported}\\t${Sample_MD5}" > genoDB-data.txt
echo -e "${ProjectID}-${loci}\\t${chrom}\\t${loci}\\t${genoDB_size}\\t${genoDB_verified}\\t${N_expected}\\t${N_imported}\\t${Sample_MD5}" > ${wrk}/genoDB-data.txt


# Copy logs
${RCLONE} --config=${dataDir}/scripts/joint_calling.conf copyto ${wrk}/genoDB-data.txt genodb_output:Checks/${chrom}/${loci}/genoDB-data.txt
${RCLONE} --config=${dataDir}/scripts/joint_calling.conf copyto ${wrk}/SM.txt genodb_output:Checks/${chrom}/${loci}/SM.txt
${RCLONE} --config=${dataDir}/scripts/joint_calling.conf copyto ${wrk}/${ProjectID}-${loci}.Joint-Calling.log genodb_output:Checks/${chrom}/${loci}/${ProjectID}-${loci}.Joint-Calling.log
${RCLONE} --config=${dataDir}/scripts/joint_calling.conf copyto ${wrk}/${ProjectID}-${loci}.genoDB.log genodb_output:Checks/${chrom}/${loci}/${ProjectID}-${loci}.genoDB.log
${RCLONE} --config=${dataDir}/scripts/joint_calling.conf copyto ${wrk}/${ProjectID}_${loci}.imported.txt genodb_output:Checks/${chrom}/${loci}/${ProjectID}_${loci}.imported.txt



#####################################
#####################################
# 
# Compress Archive DB to dCache
# 
#####################################
#####################################


# Archive & push to dCache
echo -e "\\n\\nArchiving genoDB for dCache upload\\n\\n"
bash ${dataDir}/scripts/manage_genoDB.sh ${wrk} ${ProjectID}-${loci} ${wrk}/${ProjectID}-${loci}
${RCLONE} --config=${dataDir}/scripts/joint_calling.conf copyto ${wrk}/${ProjectID}-${loci}.tar.gz genodb_output:genoDB/${chrom}/${loci}/${ProjectID}-${loci}.tar.gz



# Clean up
echo -e "\\nProcessing complete, clearing working directory"
cd ${wrk}
cd ../
# rm -fr ${loci}
