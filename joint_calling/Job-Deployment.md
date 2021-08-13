# Scale Out



## Setup Results Directories & PiCaS Design Document

```bash
# Set useful variables
. /cvmfs/softdrive.nl/projectmine_sw/software/bin/data_processing/job-variables.sh
view="JointCalling"
wrk=~/data_processing/${view}
SRM_Dir=srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/lsgrid/Project_MinE
Gloubs_Dir=gsiftp://gridftp.grid.sara.nl/pnfs/grid.sara.nl/data/lsgrid/Project_MinE
SRM_Callset=${projectSRM}/process_gvcf/Callsets
Globus_Callset=${project}/process_gvcf/Callsets
db=/cvmfs/softdrive.nl/projectmine_sw/software/bin/data_processing/meta.db
mkdir -p ${wrk} && cd ${wrk}


# Set pipeline specific variables
mode="WXS"
ProjectID="DF3_WXS"
out=${Globus_Callset}/${mode}/${ProjectID}
outSRM=${SRM_Callset}/${mode}/${ProjectID}


# Create directories
srmmkdir ${projectSRM}/process_gvcf/
srmmkdir ${projectSRM}/process_gvcf/Callsets
srmmkdir ${projectSRM}/process_gvcf/Callsets/${mode}
srmmkdir ${outSRM}
srmmkdir ${outSRM}/VCF
srmmkdir ${outSRM}/genoDB
srmmkdir ${outSRM}/Checks

for chrom in chr{1..22} chr{X..Y}
	do
	srmmkdir ${outSRM}/VCF/${chrom}
	srmmkdir ${outSRM}/genoDB/${chrom}
done
```



## Schedule Work

### i). Setup Data to Process

```bash
# List all samples
echo -e "select SM, '${Gloubs_Dir}/' || b38_gVCF from b38_SM where b38_gVCF like '%Country%' and Projectt = 'MinE' and Site in ('Cirulli', 'UMass', 'Australia-Brisbane', 'Chinese-MND', 'fALS') order by random();" | sqlite3 ${db} > ${wrk}/${ProjectID}.list
echo -e "select SM, '${Gloubs_Dir}/' || b38_gVCF from b38_SM where b38_gVCF like '%Country%' and Projectt = 'dbGaP';" | sqlite3 ${db} >> ${wrk}/${ProjectID}.list
sort -R ${wrk}/${ProjectID}.list > tmp
mv tmp ${wrk}/${ProjectID}.list

uberftp -rm ${out}/VCF/${ProjectID}.list
globus-url-copy -c -cd file://${wrk}/${ProjectID}.list ${out}/VCF/${ProjectID}.list


# Stage data
rm -f ${wrk}/${ProjectID}.staging.txt
toStage=$( cut -d \/ -f 9- ${wrk}/${ProjectID}.list | awk ' { print "'${SRM_Dir}'/"$1"\n'${SRM_Dir}'/"$1".tbi" } ' | xargs)
nohup srm-bring-online -lifetime=1209600 ${toStage} &>> ${wrk}/${ProjectID}.staging.txt &
```



### ii). Create tokens

Example token schema

![Example-Token](C:\Users\kenna\OneDrive\Documents\GitHub\data_processing\joint_calling\Example-Token.PNG)



```bash
# Create Token Import Script
echo -e "cd /cvmfs/softdrive.nl/projectmine_sw/software/bin/data_processing/joint_calling/" > ${wrk}/Upload-Tokens-${ProjectID}.sh
for tgt in $( ${soft}/software/bin/tree -fi ${b38}/${mode} | grep "bed$" | sort -R )
	do

	# Set task vars
	shard=$(basename ${tgt} | sed 's/.bed//g' | sed 's/.shard/_shard/g')
	chrom=$(dirname ${tgt} | awk 'BEGIN{FS="/"} {print $NF}')

	# Create token: 	View, gVCF-List, ProjectID, Loci, Shard, WXS|WGS
	echo -e "/usr/bin/python create_tokens.Final.py \"JointCalling\" \"${out}/VCF/${ProjectID}.list\" \"${ProjectID}\" \"${tgt}\" \"${shard}\" \"${mode}\" " >> ${wrk}/Upload-Tokens-${ProjectID}.sh
done

# Import tokens
nohup bash ${wrk}/Upload-Tokens-${ProjectID}.sh &>> ${wrk}/Upload-Tokens-${ProjectID}.log &
less ${wrk}/Upload-Tokens-${ProjectID}.log
```



### iii). Write JDL & Submit Jobs to WMS

```bash
# Write Parametric JDL for WMS
sleep 10m

echo -e "
JobType = \"Parametric\";
Parameters = 11 ;
ParameterStart = 0 ;
ParameterStep = 1 ;
// Job Resources
Requirements = ( RegExp(\"gina\",other.GlueCEUniqueID) && other.GlueCEPolicyMaxWallClockTime >= 2880 );
SMPgranularity = 2;
CpuNumber = 2;
ShallowRetryCount = 0;
RetryCount = 0;
// Executable
Executable = \"/bin/bash\";
Arguments = \"/cvmfs/softdrive.nl/projectmine_sw/software/bin/data_processing/job-script.sh ${view}\";
// Standard Output and Error
StdOutput = \"JointCalling-Parametric.txt\";
StdError = \"JointCalling-Parametric.txt\";
// Output Sandbox Contents
OutputSandbox = { \"JointCalling-Parametric.txt\"};
" > ${wrk}/${view}-Parametric.jdl



# Submit jobs
for i in {1..6}
	do
	glite-wms-job-submit -d ${USER} -o ${wrk}/${view}-Parametric.txt ${wrk}/${view}-Parametric.jdl
	sleep 2s
done

```



### iv). Check progress

```bash
# Inspect results
Disk_Results=Disk/User/${USER}/projects/process_gvcf/Callsets/${mode}/${ProjectID}
Tape_Results=Tape/User/${USER}/projects/process_gvcf/Callsets/${mode}/${ProjectID}


# Merge all progress tables
cd /projectmine-nfs/${Disk_Results}
echo -e "genoDB_ID\\tChromosome\\tLoci\\tgenoDB_Size\\tJoint_Calling_Verified\\tN_Expected\\tN_Imported\\tSample_List_md5sum" > ${wrk}/genoDB-Progress.txt
cat $(${soft}/software/bin/tree -fi Checks/ | grep "genoDB-data.txt" | sort -R | xargs) >> ${wrk}/genoDB-Progress.txt

less ${wrk}/genoDB-Progress.txt
```
