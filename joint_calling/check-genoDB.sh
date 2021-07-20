#!/bin/bash


# Set vars
. ${TMPDIR}/mine_wgs_processing/job_management/start.sh
ref=${b38}/hs38DH.fa
TREE=/cvmfs/softdrive.nl/bkenna/Miniconda2/bin/tree
ProjectID=$1
genome=$2
tgt=$3
genoDB=$4
outTape=$(echo $genoDB | sed 's/\/genoDB.*//g')
outDisk=$(echo $genoDB | sed 's/\/genoDB.*//g' | sed 's/Tape/Disk/g')
loci=$(basename ${tgt} | sed 's/.bed//g')
chrom=$(echo ${loci} | cut -d _ -f 1)
wrk=${TMPDIR}/${ProjectID}_${loci}
mkdir -p ${wrk}
cd ${wrk}


#########################################################
#########################################################
# 
# Check genoDB
# 
#########################################################
#########################################################


# Download and unpack
echo -e "\\nDownloading & Unpacking genoDB\\n\\n"
globus-url-copy ${outDisk}/VCF/${ProjectID}.list file://${wrk}/
globus-url-copy ${genoDB} file://${wrk}/

if [ ! -f ${ProjectID}-${loci}.tar.gz ]
then
	echo -e "\\nExiting, no genoDB shard exists for ${ProjectID}-${loci}"
	cd ..
	rm -fr rm -fr ${ProjectID}_${loci}
	exit

else

	# Check archived data
	tar -xf ${ProjectID}-${loci}.tar.gz && rm -f ${ProjectID}-${loci}.tar.gz
	if [ `${TREE} -fi genoDB/ | grep -c "gz$"` -le 10 ]
	then

		# Manage genoDB
		echo -e "\\ngenoDB data not compressed, runing manage_genoDB.sh"
		bash ~/project/pipelines/mine_wgs_processing/job_management/joint_calling/manage_genoDB.sh ${wrk} ${ProjectID} ${wrk}/${ProjectID}-${loci}
		uberftp -rm ${genoDB}
		globus-url-copy file://${wrk}/${ProjectID}-${loci}.tar.gz ${genoDB}

	# Decompress
	else
		echo -e "\\nDecompressing genoDB"
		bash Decompression.sh
		rm -f Decompression.sh compression.sh
	fi
fi


# Test joint calling & pull sample list
echo -e "\\nTesting Joint Calling\\n\\n"
testLoci=$(head -n 1 ${tgt} | awk '{print $1":"$2"-"$2+50}')
java -Djava.io.tmpdir=${wrk} -jar ${gatk4} GenotypeGVCFs -R ${ref} -O ${wrk}/${ProjectID}-${loci}.vcf.gz -D ${dbSNP38} -G StandardAnnotation -G AS_StandardAnnotation -V gendb://${wrk}/genoDB/${ProjectID}-${loci} -L ${testLoci} &>> ${wrk}/${ProjectID}-${loci}.Joint-Calling.log
tabix -h ${wrk}/${ProjectID}-${loci}.vcf.gz donkey | tail -n 1 | cut -f 10- | sed 's/\t/\n/g' > SM.txt


# Print metrics
echo -e "\\nCollecting genoDB metrics\\n\\n"
genoDB_size=$(du -sh genoDB/* | awk '{print $1}')
genoDB_verified=$(if [ -f ${wrk}/${ProjectID}-${loci}.vcf.gz.tbi ]; then echo "1"; else echo "0"; fi)
N_expected=$(cat ${ProjectID}.list | wc -l)
N_imported=$(cat SM.txt | wc -l)
Sample_MD5=$(cat SM.txt | md5sum - | awk '{print $1}')
echo -e "${ProjectID}-${loci}\\t${chrom}\\t${loci}\\t${genoDB_size}\\t${genoDB_verified}\\t${N_expected}\\t${N_imported}\\t${Sample_MD5}" > genoDB-data.txt


# Push results
echo -e "file://${wrk}/SM.txt ${outDisk}/Checks/${chrom}/${loci}/SM.txt\\nfile://${wrk}/genoDB-data.txt ${outDisk}/Checks/${chrom}/${loci}/genoDB-data.txt" > Transfers.txt
awk '{print $2}' Transfers.txt | while read line; do uberftp -rm ${line}; done
globus-url-copy -c -cd -f ${wrk}/Transfers.txt
cd ..
rm -fr ${ProjectID}_${loci}
echo -e "\\nDone, results pused to ${outDisk}/Checks/${chrom}/${loci}\\n\\n"
