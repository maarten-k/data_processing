#!/bin/bash


# Parse args
genoDB=$1 # Path to where the "genoDB" directory is
ProjectID=$2
out=$3 # Path + Prefix Name for the resulting archive
soft=/cvmfs/softdrive.nl/projectmine_sw/software/bin
cd ${genoDB}


# Write compression script
rm -f compression.sh && touch compression.sh
${soft}/tree -fi genoDB/ | grep "\.[a-Z]*[a-Z]$" | grep -v "book_keeping" | while read line
	do
	echo -e "gzip '${line}'" >> compression.sh
done 


# Execute
echo -e "\\nCompressing data\\n"
/usr/bin/time bash compression.sh


# Write a decompression script
rm -f Decompression.sh && touch Decompression.sh
${soft}/tree -fi genoDB/ | grep "\.[a-Z]*[a-Z]$" | grep -v "book_keeping" | while read line
	do
	echo -e "gzip -d '${line}'" >> Decompression.sh
done


# Archive the compressed genoDB + retrevial script: Decompress for Parent process
tar -czf ${out}.tar.gz Decompression.sh compression.sh genoDB/
/usr/bin/time bash Decompression.sh
rm -fr Decompression.sh compression.sh


# Log out
results=$(du -sh ${out}.tar.gz | awk '{print $2" = "$1}')
echo -e "\\n\\nArchiving completed, data stored in ${results}\\n\\n"
