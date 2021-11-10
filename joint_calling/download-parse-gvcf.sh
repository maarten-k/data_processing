#!/bin/bash


# Set args
. /cvmfs/softdrive.nl/projectmine_sw/software/bin/data_processing/job-variables.sh
gvcfList=$1
tgt=$2
loci=$(basename ${tgt} | sed 's/.bed//g')
wrk=$(dirname ${gvcfList})
out=${wrk}/Parsed_gVCFs


# Setup
mkdir -p ${wrk}/In_gVCFs ${out}
cd ${wrk}/In_gVCFs
touch ${wrk}/Samples-Dropped.txt

#geneate mapping from chomomsome name in the vcf without the chr prefix to a one with chr prefix
#this is needed for ukbiobank files
rm -f number2chr_conv.txt 
for i in {1..22} X Y MT; do echo "$i chr$i" >> number2chr_conv.txt;done
sed 's@^chr@@g' ${tgt} > ${tgt}without_chr 


# Parse gVCF until globus is done
Nexpected=$(ls *gz | wc -l)
ls *gz | while read gvcf
	do

	# Parse active loci
	if [ ! -f ${gvcf}.tbi ]; then ${TABIX} -p vcf -f ${gvcf}; else touch ${gvcf}.tbi; fi

    if [ $(${TABIX} -l ${gvcf} |head -n 1) == "chr1" ];then 
	${TABIX} ${gvcf} -h -R ${tgt} | ${BCFTOOLS} sort -O v |uniq |bgzip -l1 > ${out}/tmp.subset.g.vcf.gz
	else
	#this is needed for uk biobank samples due missing chr prefix. First selecting and then fix annotion is faster for fixing anotation
	${TABIX} ${gvcf} -h -R ${tgt}without_chr  | ${BCFTOOLS} sort -O v |uniq |${BCFTOOLS} annotate --rename-chrs number2chr_conv.txt|bgzip -l1 > ${out}/tmp.subset.g.vcf.gz
	fi
	mv ${out}/tmp.subset.g.vcf.gz ${out}/${gvcf}
	${TABIX} -p vcf -f ${out}/${gvcf}
	rm -f ${gvcf} ${gvcf}.tbi

	# Manage crap
	if [ `${TABIX} ${out}/${gvcf} -R ${tgt} |head| wc -l` -le 1 ]
	then
		echo "failed to shard ${gvcf}"
		rm -f ${out}/${gvcf} ${out}/${gvcf}.tbi
		echo -e "${gvcf}" >> ${wrk}/Samples-Dropped.txt
	fi
done


# Log status
cd ..
rm -fr In_gVCFs
Ndropped=$(wc -l ${wrk}/Samples-Dropped.txt)
Ngvcf=$(ls ${out}/*gz | wc -l)
if [ ${Nexpected} == ${Ngvcf} ]
then
	echo -e "\\n\\nAll gVCF Successfully Downloaded, unable to parse = ${Ndropped}\\n"
else
	echo -e "\\n\\nError, ${Ngvcf} were parsed out of ${Nexpected}, unable to parse = ${Ndropped}\\n"
fi
