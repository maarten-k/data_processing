#/bin/bash


# Set args
task=$1
out=$2
stopper=$3


# Push to dCache
uberftp -rm ${out}
globus-url-copy -c -cd file://${task} ${out}


# Push log file to out while modified is true or 33hr threshold is reached
threshold=400
count=0
while [ ! -f ${stopper} ]
	do

	# Break if too many iterations
	if [ ${count} -ge ${threshold} ]; then break; fi

	# Sleep, push, iter
	sleep 5m
	uberftp -rm ${out}
	globus-url-copy -c -cd file://${task} ${out}
	count=$((${count} + 1))

done
