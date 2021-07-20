#!/bin/bash


# Set var
RCLONE=~/rclone-v1.54.0-linux-amd64/rclone
TREE=~/tree-1.7.0/tree
BGZIP=/hpc/hers_en/bkenna/software/miniconda3/bin/bgzip
gatk4=/hpc/hers_en/bkenna/software/gatk-4.1.4.0/gatk-package-4.1.4.0-local.jar
module load tabix/0.2.6 Java/1.8.0_60
ref=/hpc/hers_en/bkenna/software/reference/hs38DH.fa
dbSNP=/hpc/hers_en/bkenna/software/reference/dbsnp_146.hg38.vcf.gz
dataDir=/hpc/hers_en/bkenna/callsets
ProjectID=$1
tgt=$2
sampleList=$3
copyMode=${4:-0}


# Set task specific vars
loci=$(basename ${tgt} | sed 's/.bed//g' | sed 's/.shard/_shard/g')
chrom=$(echo ${loci} | cut -d _ -f 1)
GATK_Loci_ARG=$((head -n 1 ${tgt} | cut -f 1-2; tail -n 1 ${tgt} | cut -f 3) | xargs | awk '{print $1":"$2"-"$3}')
wrk=${dataDir}/${ProjectID}/${chrom}/${loci}
mkdir -p ${wrk}/genoDB ${wrk}/Logs ${wrk}/VCF ${wrk}/Parsed_gVCFs ${wrk}/In_gVCFs
cd ${wrk}


# Download genoDB
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


# Check that all required samples are present
sed 's/,/\n/g' genoDB/${ProjectID}-${loci}/callset.json | grep -wo "sample.*" | cut -d \: -f 2 | cut -d \" -f 2 | awk '{ print $1}' > imported.txt
for sm in $(sort -R $sampleList )
	do
	if [ -z `awk '$1 ~ /^'${sm}'$/ {print $1}' imported.txt | head -n 1` ]
	then
		echo -e "\\nExiting, samples = ${sm} not yet imported"
		rm -fr genoDB
		exit
	fi
done


# Query samples
samples=$(sed 's/^/-sn /g' ${sampleList} | xargs)
java -Djava.io.tmpdir=${wrk} -Xmx35G -jar ${gatk4} SelectVariants -R ${ref} -V gendb://${wrk}/genoDB/${ProjectID}-${loci} ${samples} -O ${wrk}/${ProjectID}-${loci}.g.vcf.gz
rm -fr genoDB


# Generate callset
java -Djava.io.tmpdir=${wrk} -Xmx37G -jar ${gatk4} GenotypeGVCFs -R ${ref} -O ${wrk}/${ProjectID}-${loci}.vcf.gz -D ${dbSNP} -G StandardAnnotation -G AS_StandardAnnotation -G AlleleSpecificAnnotation -A AlleleFraction -V ${wrk}/${ProjectID}-${loci}.g.vcf.gz
rm -f ${ProjectID}-${loci}.g.vcf.gz ${ProjectID}-${loci}.g.vcf.gz.tbi
