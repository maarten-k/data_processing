#!/bin/bash


# Set var
. ${soft}/data_processing/start.sh
inp=$1
acc=$(basename ${inp} | cut -d \. -f 1)
base=$(basename ${inp})


# Sanity check gVCF
iid=$(zcat ${inp} | head -n 10000 | grep "#CHROM" | cut -f 10)
size=$(du -sh ${inp} | awk '{print $1}')
${TABIX} -R ${tgt} ${inp} | bgzip -c > ${base}-exome-query.vcf.gz

length=$(zcat ${base}-exome-query.vcf.gz | wc -l)
width=$(zcat ${inp} | grep -v "\#" | awk '{print NF}' | sort | uniq -c | awk '{print $2}' | sed 's/\n/,/g')
NVar=$(zgrep -c "MQ" ${base}-exome-query.vcf.gz)

# Summarise variants
GQ20=$(zgrep "MQ" ${base}-exome-query.vcf.gz | cut -f 10 | cut -d \: -f 4 | sort -n | awk '$1 > 20 {print}' | wc -l)
GQ60=$(zgrep "MQ" ${base}-exome-query.vcf.gz | cut -f 10 | cut -d \: -f 4 | sort -n | awk '$1 > 60 {print}' | wc -l)
GQ90=$(zgrep "MQ" ${base}-exome-query.vcf.gz | cut -f 10 | cut -d \: -f 4 | sort -n | awk '$1 > 90 {print}' | wc -l)
variantSummary=${GQ20},${GQ60},${GQ90}
rm -f ${base}-exome-query.vcf.gz

# Create table
echo -e "IID\\tAccession\\tgVCF\\tDisk_Usage\\tWidth\\tLength\\tN_Variants\\tN_dbSNP_Calls\\tGenome_GQ_Summary(GT_20,GT_60,GT_90)\\tVariant_GQ_Summary(GT_20,GT_60,GT_90)" > ${base}_checks.tsv
echo -e "${iid}\\t${acc}\\t${base}\\t${size}\\t${width}\\t${length}\\t${NVar}\\t${variantSummary}" >> ${base}_checks.tsv
