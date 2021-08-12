#!/bin/bash


# Load vars
. ${soft}/software/bin/data_processing/job-variables.sh
ref=${b38}/hs38DH.fa
PLINK=${soft}/software/bin/plink
VCFTOOLS=/cvmfs/softdrive.nl/bkenna/Miniconda2/bin/vcftools
ProjectID=$1
tgt=$2
genome=$3
genoDB=$4
loci=$(basename ${tgt} | sed 's/.bed//g' | sed 's/.shard/_shard/g')
chrom=$(echo ${loci} | cut -d _ -f 1)
out=${project}/process_gvcf/Callsets/${genome}/${ProjectID}/VCF/Callset-Check/${chrom}
wrk=${TMPDIR}/${ProjectID}_${loci}
mkdir -p ${wrk} && cd ${wrk}


# Get genoDB
echo -e "\\nDownloading genoDB"
globus-url-copy ${genoDB} file://${wrk}/
tar -xf ${ProjectID}-${loci}.tar.gz
rm -f ${ProjectID}-${loci}.tar.gz
bash Decompression.sh


# Query callset
echo -e "\\nQuerying callset for 10 random sites"
# sort -R ${tgt} | head -n 1 | awk '{print $1"\t"$2"\t"$2+1000}' | sort -nk 2 > test_loci.bed
sort -R ${tgt} | awk 'NR <= 65' | sort -nk 2 > test_loci.bed
java -Djava.io.tmpdir=${wrk} -Xmx37G -jar ${gatk4} GenotypeGVCFs -R ${ref38} -O ${wrk}/${ProjectID}-${loci}.vcf.gz -D ${dbSNP38} -G StandardAnnotation -G AS_StandardAnnotation -G AlleleSpecificAnnotation -A AlleleFraction -V gendb://${wrk}/genoDB/${ProjectID}-${loci} -L ${wrk}/test_loci.bed &>> ${wrk}/${ProjectID}-${loci}.Test.log
rm -fr genoDB tmp* *compression*
echo -e "\\n\\n\\n\\n" >> ${wrk}/${ProjectID}-${loci}.Test.log


# Extract bi-allelic variants overlapping dbSNP sites with GQ > 20
echo -e "\\nJoint calling complete, fetching bi-allelic dbSNP SNVs with GQ > 20"
zcat ${wrk}/${ProjectID}-${loci}.vcf.gz | cut -f 1-9 | grep -P "\trs" | cut -f 3 > snps.txt
${VCFTOOLS} --gzvcf ${wrk}/${ProjectID}-${loci}.vcf.gz --recode --recode-INFO-all --minGQ 20 --snps snps.txt --remove-indels --min-alleles 2 --max-alleles 2 --out ${wrk}/${ProjectID}-${loci} &>> ${wrk}/${ProjectID}-${loci}.Test.log
bgzip -c ${wrk}/${ProjectID}-${loci}.recode.vcf > ${wrk}/${ProjectID}-${loci}.vcf.gz
tabix -p vcf -f ${wrk}/${ProjectID}-${loci}.vcf.gz
rm -f ${wrk}/${ProjectID}-${loci}.recode.vcf snps.txt
echo -e "\\n\\n\\n\\n" >> ${wrk}/${ProjectID}-${loci}.Test.log


# Analyze data: Sample missing rate < 25%, Missing genotype rate < 10%
echo -e "\\nConverting to plink and analyzing results"
${PLINK} --vcf ${wrk}/${ProjectID}-${loci}.vcf.gz --double-id --make-bed --out ${ProjectID}_${loci} --geno 0.1 --mind 0.85 &>> ${wrk}/${ProjectID}-${loci}.Test.log
rm -f ${wrk}/${ProjectID}-${loci}.vcf.gz ${wrk}/${ProjectID}-${loci}.vcf.gz.tbi
N_Fail=$(cat ${ProjectID}_${loci}.irem | wc -l)
GTR=$(grep "Total genotyping rate in remaining samples" ${ProjectID}_${loci}.log | awk '{print $NF}' | sed 's/\.$//g')
N_Samples=$(wc -l ${ProjectID}_${loci}.fam | awk '{print $1}')
N_Variants=$(wc -l ${ProjectID}_${loci}.bim | awk '{print $1}')
Sample_MD5=$(awk '{print $1}' ${ProjectID}_${loci}.irem | md5sum - | awk '{print $1}')
if [ ${N_Fail} -le 10 ]
then
	FailedSamples=$(awk '{print $1}' ${ProjectID}_${loci}.irem | xargs | sed 's/ /,/g')
fi
echo -e "${ProjectID}-${loci}\\t${N_Samples}\\t${N_Variants}\\t${GTR}\\t${N_Fail}\\t${Sample_MD5}\\t${FailedSamples}" > ${ProjectID}_${loci}.results.txt


# Archive and push results
echo -e "\\nAnalysis complete, archiving results"
cd ..
tar --exclude="*vcf*" -czf ${ProjectID}-${loci}.plink.tar.gz ${ProjectID}_${loci}/
echo -e "file://${TMPDIR}/${ProjectID}-${loci}.plink.tar.gz ${out}/${ProjectID}-${loci}.plink.tar.gz\\nfile://${wrk}/${ProjectID}_${loci}.results.txt ${out}/${ProjectID}_${loci}.results.txt" > ${wrk}/Transfers.txt
awk '{print $2}' ${wrk}/Transfers.txt | while read line; do uberftp -rm ${line}; done
globus-url-copy -c -cd -f ${wrk}/Transfers.txt


# Clean up
echo -e "\\nProcess complete, clearing temporary directory"
rm -fr ${ProjectID}-${loci}.plink.tar.gz ${ProjectID}_${loci}/
