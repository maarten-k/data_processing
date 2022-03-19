#!/bin/bash


# Load vars
. /cvmfs/softdrive.nl/projectmine_sw/software/bin/data_processing/job-variables.sh


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
set -ex
mkdir -p ${wrk}/In_gVCFs ${wrk}/Parsed_gVCFs ${wrk}/genoDB
cd ${wrk}
set +e

###############################################################################################################
###############################################################################################################
# 
# Generate GenoDB
# 
###############################################################################################################
###############################################################################################################


# Download gvcf list + loci
echo -e "\\n\\nFetching Group gVCF List & Active Loci\\n"
echo -e "file://${tgt} file://${wrk}/${loci}.bed\\n${gVCFs} file://${wrk}/${ProjectID}.list" > ${wrk}/Transfer.txt
/usr/bin/time -f 'timiming: %C "%E real,%U user,%S sys CPU Percentage: %P maxres: %M' globus-url-copy -rst-retries 10 -rst-timeout 3 -f ${wrk}/Transfer.txt
sort -R ${wrk}/${ProjectID}.list > tmp
mv tmp ${wrk}/${ProjectID}.list


# Exit if downloads failed
echo -e "\\nChecking loci & gVCF list have downloaded\\n"
if [ `cat ${wrk}/${ProjectID}.list | wc -l` -eq 0 ] || [ `cat ${wrk}/${loci}.bed | wc -l` -eq 0 ]
then
	echo -e "\\n\\nExiting, unable to download loci or gVCF list\\n"
	cd ${TMPDIR}
	rm -fr ${ProjectID}_${loci}/
	exit
fi


# Check whether to run in update mode
echo -e "\\nChecking whether a database for active loci has been created for this project\\n"
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
	cut -d \| -f 2 ${wrk}/${ProjectID}.list | sort -R | awk '{print $1"\n"$1".tbi"}' | awk -F '/' '{print $0" file://'${wrk}'/In_gVCFs/"$NF}' |sed 's@_23161_0_0.gvcf.gz$@.g.vcf.gz@g'|sed 's@_23161_0_0.gvcf.gz.tbi$@.g.vcf.gz.tbi@g'|sed 's@.WXS.g.vcf.gz$@.g.vcf.gz@g'|sed 's@.WXS.g.vcf.gz.tbi$@.g.vcf.gz.tbi@g'|sed 's@_exome_extract.g.vcf.gz$@.g.vcf.gz@g'|sed 's@_exome_extract.g.vcf.gz.tbi$@.g.vcf.gz.tbi@g' | awk 'NR%2000 == 1 { out="'${wrk}'/'${ProjectID}'-gVCF-"++i".list"} { print > out }'
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
		/usr/bin/time -f 'timing: %C "%E real,%U user,%S sys CPU Percentage: %P maxres: %M' globus-url-copy -rst-retries 10 -rst-timeout 3 -c -cd -concurrency 4 -f ${batch} &>> /dev/null


		# Parse gVCF if exome, otherwise dowload
		if [ "${genome}" != "WGS" ]
		then
			/usr/bin/time -f 'timing: %C "%E real,%U user,%S sys CPU Percentage: %P maxres: %M' bash ${soft}/software/bin/data_processing/joint_calling/download-parse-gvcf.sh ${batch} ${wrk}/${loci}.bed ${wrk}/gVCF-download.txt
		else
			mv ${wrk}/In_gVCFs/* ${wrk}/Parsed_gVCFs/
		fi


		# Import parsed gVCF
		bash ${soft}/software/bin/data_processing/joint_calling/genoDB-Import.sh ${ProjectID} ${wrk}/${loci}.bed ${wrk}/Parsed_gVCFs ${batch}  &>> ${wrk}/GenoDB-Logging.txt
		import_exit_status=$?
		if [ "$import_exit_status" -ne 0 ];then
			echo "exit status of genoDB-Import.sh is $import_exit_status . exiting since this is not zero"
			cat ${wrk}/GenoDB-Logging.txt
			exit 1
		fi
		rm -f ${batch}
		rm -fr In_gVCFs Parsed_gVCFs
	done


# Otherwise run update mode
else

	# Download
	echo -e "${out}/genoDB/${chrom}/${loci}/${ProjectID}-${loci}.tar.gz file://${wrk}/${ProjectID}-${loci}.tar.gz\\n${outDisk}/Logs/genoDB/${chrom}/${loci}/${ProjectID}_${loci}.imported.txt file://${wrk}/${ProjectID}_${loci}.imported.txt" > ${wrk}/Transfer.txt
	/usr/bin/time -f 'timing: %C "%E real,%U user,%S sys CPU Percentage: %P maxres: %M'  globus-url-copy -cd -c -f ${wrk}/Transfer.txt
	rm -f ${wrk}/Transfer.txt

	echo "Unpacking Genodb: $(date)"
	# Unpack genoDB
	/cvmfs/softdrive.nl/projectmine_sw/software/bin/tar -I /cvmfs/softdrive.nl/projectmine_sw/software/bin/zstd  -xf ${ProjectID}-${loci}.tar.gz && rm -f ${ProjectID}-${loci}.tar.gz
	bash Decompression.sh
	rm -f Decompression.sh compression.sh
	sed 's/,/\n/g' genoDB/${ProjectID}-${loci}/callset.json | grep -wo "sample.*" | cut -d \: -f 2 | cut -d \" -f 2 | awk '{ print $1"\t'${wrk}'/"$1".g.vcf.gz"}' > ${wrk}/${ProjectID}_${loci}.imported.txt

	echo "FilterGVCF Genodb: $(date)"
	# Filter gVCF list
	# cut -d \| -f 2 ${wrk}/${ProjectID}.list | sort -R | awk '{print $1"\n"$1".tbi"}' | awk -F '/' '{print $0" file://'${wrk}'/In_gVCFs/"$NF}'|sed 's@_exome_extract.g.vcf.gz$@.g.vcf.gz@g'|sed 's@_exome_extract.g.vcf.gz.tbi$@.g.vcf.gz.tbi@g|sed 's@.WXS.g.vcf.gz$@.g.vcf.gz@g'|sed 's@.WXS.g.vcf.gz.tbi$@.g.vcf.gz.tbi@g''  > ${wrk}/${ProjectID}-gVCF.list
	# touch tmp
	# awk -F "/" '{print "/"$NF}' ${ProjectID}-gVCF.list | grep -v "tbi" | while read gvcf
	# 	do
	# 	gvcfregex=$(echo $gvcf|sed 's@.g.vcf.gz$@(_exome_extract)?.g.vcf.gz$@g')
		
	# 	if [ -z $(grep -P "${gvcfregex}" ${wrk}/${ProjectID}_${loci}.imported.txt | awk 'NR == 1 {print $1}') ]
	# 	then
	# 		grep -P "${gvcfregex}" ${wrk}/${ProjectID}.list >> tmp
			
	# 	fi
	# done
	
	imported=${wrk}/${ProjectID}_${loci}.imported.txt
	vcf_list=${wrk}/${ProjectID}.list
	echo "PWD=${PWD}"
	cat <(cut -f1 "$imported"|cut -f1 -d".") <(cut -f1 -d"|" "$vcf_list"|cut -f 1 -d".") |sort|uniq -u| awk '{print( $0)}' > samples_to_add.list
	echo "found $(wc -l samples_to_add.list) samples to add"
	#use python dict here since grep can not handle large numner of regex (needed for matching begin of string)
	python -c 'z={x.split("|")[0].split(".")[0]:x.strip() for x in  open("Test_Exome.list").readlines()}; q=[print(z[x.strip()]) for x in  open("samples_to_add.list","r").readlines()]' > tmp
	mv tmp ${ProjectID}.list
	if [ ! -s "${ProjectID}".list ] ;then
		echo "No new samples to add"
		exit 0
	fi
	echo "FilterDone Genodb: $(date)"
	
	rm -f ${ProjectID}-gVCF.list

	echo "Amount of lines in list of samples to add to DB $(wc -l ${ProjectID}.list)"
	
	if [ $(wc -l ${ProjectID}.list|cut -f1 -d " ") -ne $(wc -l samples_to_add.list|cut -f1 -d " ") ];then
		echo "amount of samples to update is not equel to number of urls"
		exit 1
	fi 

	# Iterate over batches
	cut -d \| -f 2 ${ProjectID}.list |sort -R | awk '{print $1"\n"$1".tbi"}' | awk -F '/' '{print $0" file://'${wrk}'/In_gVCFs/"$NF}' |sed 's@_23161_0_0.gvcf.gz$@.g.vcf.gz@g'|sed 's@_23161_0_0.gvcf.gz.tbi$@.g.vcf.gz.tbi@g'|sed 's@_exome_extract.g.vcf.gz$@.g.vcf.gz@g'|sed 's@_exome_extract.g.vcf.gz.tbi$@.g.vcf.gz.tbi@g'|sed 's@.WXS.g.vcf.gz$@.g.vcf.gz@g'|sed 's@.WXS.g.vcf.gz.tbi$@.g.vcf.gz.tbi@g'  | awk 'NR%2000 == 1 { out="'${wrk}'/'${ProjectID}'-gVCF-"++i".list"} { print > out }'
	N=$(ls ${wrk}/${ProjectID}*gVCF*list | wc -l)
	N_Samples=$(cat ${wrk}/${ProjectID}*gVCF*[0-9]*list | grep -c "gz$")
	count=0
	if [ ${N_Samples} == 0 ]; then echo -e "\\nExiting, provided samples already exist in the DB\\n"; cd ..; rm -fr ${ProjectID}_${loci}; exit; fi
	echo -e "\\nItertatively importing N Samples = ${N_Samples} across N Batches = ${N}\\n"
	for batch in $(ls ${wrk}/${ProjectID}*gVCF*list)
		do

		# Download gVCF
		count=$((${count}+1))
		echo -e "\\nBegining batch ${count} $(date)\\n"
		mkdir -p In_gVCFs Parsed_gVCFs
		if [ "${genome}" == "WGS" ];then grep "Callset" ${batch} > tmp; mv tmp ${batch}; fi
		/usr/bin/time -f 'timing: %C "%E real,%U user,%S sys CPU Percentage: %P maxres: %M'  globus-url-copy -rst-retries 10 -rst-timeout 3 -c -cd -concurrency 4 -f ${batch}
		echo "Downloading gvcf done: $(date)"

		# Parse gVCF if exome, otherwise dowload
		if [ "${genome}" != "WGS" ]
		then
			bash ${soft}/software/bin/data_processing/joint_calling/download-parse-gvcf.sh ${wrk}/${ProjectID}-gVCF.list ${wrk}/${loci}.bed &>> ${wrk}/Parsing-Log.txt
		else
			mkdir -p Parsed_gVCFs/
			mv In_gVCFs/* Parsed_gVCFs/
		fi
		echo "Parsing gvcf done: $(date)"

		# Import parsed gVCF
		bash ${soft}/software/bin/data_processing/joint_calling/genoDB-Import.sh ${ProjectID} ${wrk}/${loci}.bed ${wrk}/Parsed_gVCFs &>> ${wrk}/GenoDB-Logging.txt
		import_exit_status=$?
		if [ "$import_exit_status" -ne 0 ];then
			echo "exit status of genoDB-Import.sh is $import_exit_status . exiting since this is not zero"
			echo "current time $(date)"
			cat ${wrk}/GenoDB-Logging.txt
			exit 1
		fi
		echo "Importing gvcf done: $(date)"

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
