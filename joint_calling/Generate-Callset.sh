#!/bin/bash


# Load vars
. ${TMPDIR}/mine_wgs_processing/job_management/start.sh
ref=${b38}/hs38DH.fa
# n=$(echo $RANDOM % 10 | bc)
# sleep ${n}s


# Parse input args
ProjectID=$1
tgt=$2
export gVCFs=$3 	# Maybe just query this?
genome=$4
updatingDB=0
loci=$(basename ${tgt} | sed 's/.bed//g' | sed 's/.shard/_shard/g')
chrom=$(echo ${loci} | cut -d _ -f 1)
wrk=${TMPDIR}/${ProjectID}_${loci}
export out=$(dirname ${gVCFs} | sed 's/\/VCF$//g' | sed 's/Disk/Tape/g')
export outDisk=$(echo -e "${out}" | sed 's/Tape/Disk/g')


# Setup directories
mkdir -p ${wrk}/In_gVCFs ${wrk}/Parsed_gVCFs ${wrk}/genoDB
cd ${wrk}


###############################################################################################################
###############################################################################################################
# 
# Generate GenoDB
# 
###############################################################################################################
###############################################################################################################


# Download gvcf list + loci
echo -e "\\n\\nFetching Group gVCF List & Active Loci\\n"
echo -e "${tgt} file://${wrk}/${loci}.bed\\n${gVCFs} file://${wrk}/${ProjectID}.list" > ${wrk}/Transfer.txt
globus-url-copy -rst-retries 10 -rst-timeout 3 -f ${wrk}/Transfer.txt
sort -R ${wrk}/${ProjectID}.list > tmp
mv tmp ${wrk}/${ProjectID}.list


# Exit if downloads failed
if [ `cat ${wrk}/${ProjectID}.list | wc -l` -eq 0 ] || [ `cat ${wrk}/${loci}.bed | wc -l` -eq 0 ]
then
	echo -e "\\n\\nExiting, unable to download loci or gVCF list\\n"
	cd ${TMPDIR}
	rm -fr ${ProjectID}_${loci}/
	exit
fi


# Check whether to run in update mode
if [ `globus-url-copy -list ${outDisk}/Logs/genoDB/${chrom}/${loci}/ | grep -c "imported"` -ge 1 ]
then
	updatingDB=1
	echo -e "\\nBacking up previous run before proceeding\\n"
	echo -e "${out}/genoDB/${chrom}/${loci}/${ProjectID}-${loci}.tar.gz ${out}/genoDB/backup/${chrom}/${loci}/${ProjectID}-${loci}.tar.gz\\n${outDisk}/Logs/genoDB/${chrom}/${loci}/${ProjectID}-${loci}.genoDB.log ${outDisk}/Logs/genoDB/backup/${chrom}/${loci}/${ProjectID}-${loci}.genoDB.log\\n${outDisk}/Logs/genoDB/${chrom}/${loci}/${ProjectID}-${loci}.imported.txt ${outDisk}/Logs/genoDB/backup/${chrom}/${loci}/${ProjectID}-${loci}.imported.txt\\n${outDisk}/Logs/genoDB/${chrom}/${loci}/${ProjectID}-${loci}.Parsing-Log.txt ${outDisk}/Logs/genoDB/backup/${chrom}/${loci}/${ProjectID}-${loci}.Parsing-Log.txt\\n${outDisk}/Logs/genoDB/${chrom}/${loci}/Samples-Dropped.txt ${outDisk}/Logs/genoDB/backup/${chrom}/${loci}/Samples-Dropped.txt" > Transfers.txt
	grep "backup" Transfers.txt | awk '{print $2}' | while read line; do uberftp -rm ${line}; done
	globus-url-copy -c -cd -f Transfers.txt
	rm -f Transfers.txt
fi


# Handle creation or update mode
if [ ${updatingDB} -eq 0 ]
then

	# Download & Parse gVCF
	echo -e "\\nRunning creation mode\\n"
	cut -d \| -f 2 ${wrk}/${ProjectID}.list | sort -R | awk '{print $1"\n"$1".tbi"}' | awk -F '/' '{print $0" file://'${wrk}'/In_gVCFs/"$NF}' | awk 'NR%2000 == 1 { out="'${wrk}'/'${ProjectID}'-gVCF-"++i".list"} { print > out }'
	touch ${wrk}/${ProjectID}_${loci}.imported.txt


	# Iteratively import gVCFs
	N=$(ls ${wrk}/${ProjectID}*gVCF*list | wc -l)
	N_Samples=$(cat ${wrk}/${ProjectID}*gVCF*list | grep -c "gz$")
	count=0
	echo -e "\\nItertatively importing N Samples = ${N_Samples} across N Batches = ${N}\\n"
	for batch in $(ls ${wrk}/${ProjectID}*gVCF*list)
		do

		# Download gVCF
		count=$((${count}+1))
		echo -e "\\nBegining batch ${count}\\n"
		mkdir -p ${wrk}/In_gVCFs ${wrk}/Parsed_gVCFs
		# if [ "${genome}" == "WGS" ];then grep "Callset" ${batch} > tmp; mv tmp ${batch}; fi
		globus-url-copy -rst-retries 10 -rst-timeout 3 -c -cd -concurrency 4 -f ${batch} &>> /dev/null


		# Parse gVCF if exome, otherwise dowload
		if [ "${genome}" != "WGS" ]
		then
			bash ${TMPDIR}/mine_wgs_processing/job_management/joint_calling/download-parse-gvcf.sh ${batch} ${wrk}/${loci}.bed ${wrk}/gVCF-download.txt &>> ${wrk}/Parsing-Log.txt
		else
			mv ${wrk}/In_gVCFs/* ${wrk}/Parsed_gVCFs/
		fi


		# Import parsed gVCF
		bash ${TMPDIR}/mine_wgs_processing/job_management/joint_calling/genoDB-Import.sh ${ProjectID} ${wrk}/${loci}.bed ${wrk}/Parsed_gVCFs ${batch} &>> ${wrk}/GenoDB-Logging.txt
		rm -f ${batch}
		rm -fr In_gVCFs Parsed_gVCFs
	done


# Otherwise run update mode
else

	# Download
	echo -e "${out}/genoDB/${chrom}/${loci}/${ProjectID}-${loci}.tar.gz file://${wrk}/${ProjectID}-${loci}.tar.gz\\n${outDisk}/Logs/genoDB/${chrom}/${loci}/${ProjectID}_${loci}.imported.txt file://${wrk}/${ProjectID}_${loci}.imported.txt" > ${wrk}/Transfer.txt
	globus-url-copy -cd -c -f ${wrk}/Transfer.txt
	rm -f ${wrk}/Transfer.txt


	# Unpack genoDB
	tar -xf ${ProjectID}-${loci}.tar.gz && rm -f ${ProjectID}-${loci}.tar.gz
	bash Decompression.sh
	rm -f Decompression.sh compression.sh
	sed 's/,/\n/g' genoDB/${ProjectID}-${loci}/callset.json | grep -wo "sample.*" | cut -d \: -f 2 | cut -d \" -f 2 | awk '{ print $1"\t'${wrk}'/"$1".g.vcf.gz"}' > ${wrk}/${ProjectID}_${loci}.imported.txt


	# Filter gVCF list
	cut -d \| -f 2 ${wrk}/${ProjectID}.list | sort -R | awk '{print $1"\n"$1".tbi"}' | awk -F '/' '{print $0" file://'${wrk}'/In_gVCFs/"$NF}' > ${wrk}/${ProjectID}-gVCF.list
	touch tmp
	awk -F "/" '{print "/"$NF}' ${ProjectID}-gVCF.list | grep -v "tbi" | while read gvcf
		do
		if [ -z `grep "${gvcf}$" ${wrk}/${ProjectID}_${loci}.imported.txt | awk 'NR == 1 {print $1}'` ]
		then
			grep "${gvcf}" ${wrk}/${ProjectID}.list >> tmp
		fi
	done
	mv tmp ${ProjectID}.list
	rm -f ${ProjectID}-gVCF.list


	# Iterate over batches
	cut -d \| -f 2 ${ProjectID}.list | awk '{print $1"\n"$1".tbi"}' | awk -F '/' '{print $0" file://'${wrk}'/In_gVCFs/"$NF}' | awk 'NR%2000 == 1 { out="'${wrk}'/'${ProjectID}'-gVCF-"++i".list"} { print > out }'
	N=$(ls ${wrk}/${ProjectID}*gVCF*list | wc -l)
	N_Samples=$(cat ${wrk}/${ProjectID}*gVCF*[0-9]*list | grep -c "gz$")
	count=0
	if [ ${N_Samples} == 0 ]; then echo -e "\\nExiting, provided samples already exist in the DB\\n"; cd ..; rm -fr ${ProjectID}_${loci}; exit; fi
	echo -e "\\nItertatively importing N Samples = ${N_Samples} across N Batches = ${N}\\n"
	for batch in $(ls ${wrk}/${ProjectID}*gVCF*list)
		do

		# Download gVCF
		count=$((${count}+1))
		echo -e "\\nBegining batch ${count}\\n"
		mkdir -p In_gVCFs Parsed_gVCFs
		if [ "${genome}" == "WGS" ];then grep "Callset" ${batch} > tmp; mv tmp ${batch}; fi
		globus-url-copy -rst-retries 10 -rst-timeout 3 -c -cd -concurrency 4 -f ${batch}


		# Parse gVCF if exome, otherwise dowload
		if [ "${genome}" != "WGS" ]
		then
			bash ${TMPDIR}/mine_wgs_processing/job_management/joint_calling/download-parse-gvcf.sh ${wrk}/${ProjectID}-gVCF.list ${wrk}/${loci}.bed &>> ${wrk}/Parsing-Log.txt
		else
			mkdir -p Parsed_gVCFs/
			mv In_gVCFs/* Parsed_gVCFs/
		fi


		# Import parsed gVCF
		bash ${TMPDIR}/mine_wgs_processing/job_management/joint_calling/genoDB-Import.sh ${ProjectID} ${wrk}/${loci}.bed ${wrk}/Parsed_gVCFs &>> ${wrk}/GenoDB-Logging.txt
		rm -f ${batch}
		rm -fr In_gVCFs Parsed_gVCFs
	done
fi
uberftp -rm ${outDisk}/Logs/genoDB/backup/${chrom}/${loci}/GenoDB-Logging.txt
globus-url-copy file://${wrk}/GenoDB-Logging.txt ${outDisk}/Logs/genoDB/backup/${chrom}/${loci}/GenoDB-Logging.txt


# Clean up
echo -e "\\nDone, clearing temporary data"
cd $TMPDIR
rm -fr ${ProjectID}_${loci}
