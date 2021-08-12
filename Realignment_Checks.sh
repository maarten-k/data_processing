#!/bin/bash


# Set needed vars & directories
. ${soft}/software/bin/data_processing/job-variables.sh

# Parse inputs
cram=$1
cramDisk=$(dirname ${cram} | sed 's/Tape/Disk/g')
SM=$2
base=$(basename ${cram})
wrk=${TMPDIR}/${SM}
mkdir -p ${wrk}
cd ${wrk}


# Query Site
rsync ${db} ./
db=$(basename ${db})
site=$(echo -e "select Site from b38_SM where SM = '${SM}';" | sqlite3 ${db})
if [ -z "${Site}" ]
then
	Site=UnMappedSite
fi
rm -f ${db}


# Handle Sites: 	Cirulli, fALS and UnMapped
if [ ! -z `echo "${site}" | grep -e "fALS" -e "Cirulli" -e 'UnMapped' -e "UMass" ` ]
then
	echo -e "\\nExiting process for site: ${site}"
	site=Netherlands/External/${site}
	genome=WXS

# Handle Sites: 	dbGaP, Sydney, Brisbane & Chinese MND
elif [ ! -z `echo "${SM}" | grep "SRR"` ]
then
	genome=WXS
	site=Netherlands/dbGaP/${site}

elif [ ! -z `echo "${site}" | grep "Sydney"` ]
then
	genome=WGS
	site=Australia

elif [ ! -z `echo "${site}" | grep "Brisbane"` ]
then
	genome=WXS
	site=Australia_Queensland

elif [ ! -z `echo "${site}" | grep "Chinese"` ]
then
	genome=WXS
	site=Chinese_MND

elif [ -z "${site}" ]
then
	echo -e "\\nError, site not found ${site}\\nExiting\\n"
	cd ..
	rm -fr ${SM}
	exit
else
	echo -e "\\nProceeding for Site:\\t${site}\\n"
	genome=WGS
fi


# Download data
echo -e "\\nDownloading Data\\n"
globus-url-copy ${cram} ${wrk}/${base}
${SAMTOOLS} index ${wrk}/${base}


# Check md5sum
if [ ` globus-url-copy -list ${cramDisk}/ | grep -c "final-gatk.cram.md5sum" ` -eq 0 ]
then

	# Download if on tape
	cramTape=$(echo "${cramDisk}" | sed 's/Disk/Tape/g')
	if [ ` globus-url-copy -list ${cramTape}/ | grep -c "final-gatk.cram.md5sum" ` -eq 1 ]
	then
		globus-url-copy ${cramTape}/${base}.md5sum ${wrk}/${base}.md5sum
		ls -lh

	# Exit if not present
	else
		echo -e "\\nError md5sum not found\\n"
		md5sum ${base} > ${base}.md5sum
	fi

# Otherwise download from disk
else
	echo -e "\\nDownloading md5sums\\n"
	globus-url-copy ${cramDisk}/${base}.md5sum ${wrk}/${base}.md5sum
	ls -lh
fi


# Sanity check download
RemoteBAM=$(/usr/bin/python ${soft}/job_management/adle32-check.py "${cram}" "remote")
LocalBAM=$(/usr/bin/python ${soft}/job_management/adle32-check.py "${wrk}/${base}" "local")
if [ "${RemoteBAM}" == "${LocalBAM}" ]
then
	echo -e "\\nAdler32 of input successfull\\n"
else
	echo -e "\\nAdler32 of input unsuccessfull\\nExiing\\n"
	cd ..
	rm -fr ${SM}
	exit
fi


# Check md5sum
echo -e "\\nChecking md5sum\\n"
/usr/bin/time md5sum -c ${wrk}/${base}.md5sum > ${wrk}/${SM}-md5-check.txt


# Push to the countries directory if md5sums are ok
if [ `grep -i "OK" ${wrk}/${SM}-md5-check.txt | grep -c cram:` -eq 1 ]
then

	# Push data
	echo -e "\\n\\nmd5sums Verified, pushing ${SM} to ${site}\\n"
	echo -e "file://${wrk}/${base} ${MinE_Tape}/Country/${site}/Realignment/bam/${SM}/${base}\\nfile://${wrk}/${base}.crai ${MinE_Tape}/Country/${site}/Realignment/bam/${SM}/${base}.crai\\nfile://${wrk}/${base}.md5sum ${MinE_Tape}/Country/${site}/Realignment/bam/${SM}/${base}.md5sum" > Transfers.txt
	globus-url-copy -c -cd -f Transfers.txt


	# Sanity check adler32: 	Clear input if ok, Clear Final & exit if bad
	echo -e "\\n\\nPush complete, verifying adler32 of Final-CRAM to Input-CRAM\\n"
	RemoteBAM_2=$(/usr/bin/python ${soft}/job_management/adle32-check.py "${MinE_Tape}/Country/${site}/Realignment/bam/${SM}/${base}" "remote")
	if [ "${RemoteBAM}" == "${RemoteBAM_2}" ]
	then
		# Remove input
		echo -e "\\nAdler32 of Final-CRAM successfull\\nRemoving input CRAM\\n"
		uberftp -rm ${cram}
		uberftp -rm ${cram}.crai
	else
		# Remove output
		echo -e "\\nAdler32 of Final-CRAM unsuccessfull\\nExiting\\n"
		uberftp -rm ${MinE_Tape}/Country/${site}/Realignment/bam/${SM}/${base}
		uberftp -rm ${MinE_Tape}/Country/${site}/Realignment/bam/${SM}/${base}.crai
		awk '{print "MD5SUMS-CHECK:\t"$0}' ${wrk}/${SM}-md5-check.txt
		cd ..
		rm -fr ${SM}
		exit
	fi

else
	# Otherwise exit
	echo -e "\\n\\nExiting, md5sum error\\n"
	awk '{print "MD5SUMS-CHECK:\t"$0}' ${wrk}/${SM}-md5-check.txt
	cd ..
	rm -fr ${SM}
	exit
fi


# Check flagstats
echo -e "\\n\\nChecking flagstats\\n"
/usr/bin/time ${SAMTOOLS} flagstat -@ 2 ${wrk}/${base} > ${wrk}/${SM}-flagstats.txt


# Print results
echo -e "\\n\\nPrinting Results\\n"
awk '{print "MD5SUMS-CHECK:\t"$0}' ${wrk}/${SM}-md5-check.txt
awk '{print "FLAGSTATS-CHECK:\t"$0}' ${wrk}/${SM}-flagstats.txt


# Clear tmp data
echo -e "\\nChecks complete, removing tmp data\\n"
cd ..
rm -fr ${SM}


# Create variant calling token
python ${TMPDIR}/mine_wgs_processing/job_management/create_tokens.Final.py "VariantCalling_HaplotypeCaller_V2" "${MinE_Tape}/Country/${site}/Realignment/bam/${SM}/${base}" "${SM}" "grch38" "${genome}"
