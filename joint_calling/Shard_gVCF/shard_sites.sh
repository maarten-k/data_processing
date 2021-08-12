#!/bin/bash

inp=$1
outDir=$2
sizeLimit=$3

cat ${inp} | while read shard
	do

	# Setup
	base=$(basename ${inp} | sed 's/.bed//g')
	chr=$(echo -e "${shard}" | awk '{print$1}')
	start_pos=$(echo -e "${shard}" | awk '{print$2}')
	end_pos=$(echo -e "${shard}" | awk '{print$3}')
	pos_length=$(echo -e "${shard}" | awk '{print$3 - $2}' )
	rm -f ${outDir}/${base}.bed
	touch ${outDir}/${base}.bed

	# Shard each chromosome sites into kb regions
	echo -e "$(date):\\t${chr}:${start_pos}-${end_pos}"
	while [ ${start_pos} -lt ${end_pos} ]
		do

		# Increment Site by 1000
		counter=$((${start_pos}+${sizeLimit}))
		echo -e "${chr}\\t${start_pos}\\t${counter}" >> ${base}.bed
		start_pos=$((${counter} + 1))
		# Set the last region
		if [ ${start_pos} -gt ${end_pos} ]
			then
			ending=$((${start_pos}-${sizeLimit}))
			echo -e "${chr}\\t${ending}\\t${end_pos}" >> ${outDir}/${base}.bed

		fi

	done

done
