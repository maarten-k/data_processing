#!/bin/bash


# List sharded samples
wrk=/home/bkenna/projects/Shard_gVCF
tgt_Dir=/cvmfs/softdrive.nl/projectmine_sw/resources/Build38/hs38DH/WGS_Loci/sub_shards/
gvcfDir=/projectmine-nfs/Disk/User/bkenna/projects/process_gvcf/Callsets/WGS/DF3_WGS/gVCFs/
mkdir -p ${wrk}/loci_progress/
~/tree-1.7.0/tree -fi ${tgt_Dir} | grep "bed$" | sort -R | while read tgt
	do
	loci=$(basename ${tgt} | sed 's/.bed//g')
	chrom=$(echo "$loci" | cut -d _ -f 1)
	mkdir -p ${wrk}/loci_progress/${chrom}/
	if [ ! -f ${wrk}/loci_progress/${chrom}/${loci}-progress.txt ]
	then
		~/tree-1.7.0/tree -fi ${gvcfDir}/${chrom}/${loci} | grep "gz$" | awk -F "/" '{print $(NF-1)"\t"$0}' > ${wrk}/loci_progress/${chrom}/${loci}-progress.txt
	fi
	wc -l ${wrk}/loci_progress/${chrom}/${loci}-progress.txt
done > ${wrk}/Sharding-Progress.txt
