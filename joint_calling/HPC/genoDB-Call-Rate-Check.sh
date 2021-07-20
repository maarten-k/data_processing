#!/bin/bash


# Load modules
module load vcftools/0.1.14 Java/1.8.0_60 tabix/0.2.6 plink/1.90
RCLONE=~/rclone-v1.54.0-linux-amd64/rclone
TREE=~/tree-1.7.0/tree
BGZIP=/hpc/hers_en/bkenna/software/miniconda3/bin/bgzip
gatk4=/hpc/hers_en/bkenna/software/gatk-4.1.4.0/gatk-package-4.1.4.0-local.jar
ref=/hpc/hers_en/bkenna/software/reference/hs38DH.fa
dbSNP=/hpc/hers_en/bkenna/software/reference/dbsnp_146.hg38.vcf.gz
dataDir=/hpc/hers_en/bkenna/callsets
ProjectID=$1
tgt=$2
chrom=$(basename ${tgt} | cut -d _ -f 1)
loci=$(basename ${tgt} | sed 's/.bed//g')
wrk=${dataDir}/${ProjectID}/${chrom}/${loci}
mkdir -p ${wrk}/call_rate
cd ${wrk}/call_rate


# Pull out bi-allelic snvs overlapping dbSNP sites with GQ>20
echo -e "\\nExtracting SNVs for call rate analysis\\n\\n"
zcat ${wrk}/${ProjectID}-${loci}.vcf.gz | cut -f 1-9 | grep -P "\trs" | cut -f 3 > snps.txt
vcftools --gzvcf ${wrk}/${ProjectID}-${loci}.vcf.gz --recode --recode-INFO-all --minGQ 20 --snps snps.txt --remove-indels --min-alleles 2 --max-alleles 2 --out ${ProjectID}-${loci} &>> ${ProjectID}-${loci}.Test.log
bgzip -c ${ProjectID}-${loci}.recode.vcf > ${ProjectID}-${loci}.vcf.gz
tabix -p vcf -f ${ProjectID}-${loci}.vcf.gz
rm -f ${ProjectID}-${loci}.recode.vcf snps.txt


# QC
echo -e "\\nPerforming analysis\\n\\n"
plink --vcf ${ProjectID}-${loci}.vcf.gz --double-id --make-bed --out ${ProjectID}_${loci} --geno 0.1 --mind 0.85 &>> ${ProjectID}-${loci}.Test.log
rm -f ${ProjectID}-${loci}.vcf.gz ${ProjectID}-${loci}.vcf.gz.tbi
GTR=$(grep "Total genotyping rate in remaining samples" ${ProjectID}_${loci}.log | awk '{print $NF}' | sed 's/\.$//g')
N_Samples=$(wc -l ${ProjectID}_${loci}.fam | awk '{print $1}')
N_Variants=$(wc -l ${ProjectID}_${loci}.bim | awk '{print $1}')
if [ ! -f ${ProjectID}_${loci}.irem ]
then
	N_Fail=0
	Sample_MD5=NULL
else
	N_Fail=$(awk 'NR > 1' ${ProjectID}_${loci}.irem | wc -l)
	Sample_MD5=$(awk '{print $1}' ${ProjectID}_${loci}.irem | md5sum - | awk '{print $1}')
	if [ ${N_Fail} -le 10 ]
	then
		FailedSamples=$(awk '{print $1}' ${ProjectID}_${loci}.irem | xargs | sed 's/ /,/g')
	fi
fi
echo -e "${ProjectID}-${loci}\\t${N_Samples}\\t${N_Variants}\\t${GTR}\\t${N_Fail}\\t${Sample_MD5}\\t${FailedSamples}" > ${ProjectID}_${loci}.results.txt


# Log done + results
echo -e "\\nDone\\n\\n"
awk '{ print "Results:"$0}' ${ProjectID}_${loci}.results.txt
