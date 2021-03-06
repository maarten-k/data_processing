# Generate GATK Genomics DB



## Setup Results Directories

```bash
# Set vars etc
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
ProjectID="Test_Exome"
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



## Sanity Check Scripts

### i). Setup Data to Process

```bash
# List gVCFs to import into GATK's Genomics DB (genoDB)
echo -e "select SM, '${Gloubs_Dir}/' || b38_gVCF from b38_SM where b38_gVCF like '%Country%' and Site = 'Cirulli' order by random() limit 100000;" | sqlite3 ${db} > ${wrk}/${ProjectID}.list
# less ${wrk}/${ProjectID}.list


# Put the gVCF import list into the projects VCF directory
echo $out
uberftp -rm ${out}/VCF/${ProjectID}.list
globus-url-copy -c -cd file://${wrk}/${ProjectID}.list ${out}/VCF/${ProjectID}.list


# Stage data
rm -f ${wrk}/${ProjectID}.staging.txt
toStage=$( cut -d \/ -f 9- ${wrk}/${ProjectID}.list | awk ' { print "/projectmine-nfs/"$1"\n/project/"$1".tbi" } ' | xargs)
nohup srm-bring-online -lifetime=1209600 ${toStage} &>> ${wrk}/${ProjectID}.staging.txt &
```



### ii). Do a Test Run

```bash
# Run test for a specific loci
WES_Loci_Dir=${b38}/WXS
chrom=chr21
Exome_Shard=$(ls ${WES_Loci_Dir}/${chrom} | sort -R | head -n 1 | sed 's/.bed//g')
tgt=${WES_Loci_Dir}/${chrom}/chr21_shard_5.bed


bash ${soft}/software/bin/data_processing/joint_calling/Generate-Callset.sh ${ProjectID} ${tgt} ${out}/VCF/${ProjectID}.list ${mode}
```



### iii). Inspect Results

```bash
# Inspect results
Disk_Results=Disk/User/${USER}/projects/process_gvcf/Callsets/${mode}/${ProjectID}
Tape_Results=Tape/User/${USER}/projects/process_gvcf/Callsets/${mode}/${ProjectID}


# List the genomics DB: Tar archive
cd /projectmine-nfs/$Tape_Results
ls -lh genoDB/chr21/chr21_shard_5/


# List non / backed-up genoDB
${soft}/software/bin/tree -fish genoDB/ | grep "tar.gz" | grep "backup"
${soft}/software/bin/tree -fish genoDB/ | grep "tar.gz" | grep -v "backup"


# Check out an importing log: Only the most recent from a job is kept
cd /projectmine-nfs/${Disk_Results}
less Logs/genoDB/chr21/chr21_shard_5/${ProjectID}-chr21_shard_5.genoDB.log


# Check out the ETL sanity check: Only the most recent from a job is kept
ls Checks/chr21/chr21_shard_5/
less Checks/chr21/chr21_shard_5/${ProjectID}-chr21_shard_5.Joint-Calling.log
wc -l Checks/chr21/chr21_shard_5/SM.txt	# Number of samples


# Check the progress table: Example is below between quotes & delimiter replaced with commas
cat Checks/chr21/chr21_shard_5/genoDB-data.txt

"
The file does not contain a header so that the 1354 tables can be concatenated

genoDB_ID,chrom,Loci,DB_Size,Joint_Calling_Verified,N_Expected_Samples,N_Import,Sample_List_md5sum
Test_Exome-chr21_shard_5,chr21,chr21_shard_5,15M,1,28,58,ebe270e06d8db7ea90decc0a9c833ce6
"

# Merge all progress tables
cd /projectmine-nfs/${Disk_Results}
echo -e "genoDB_ID\\tChromosome\\tLoci\\tgenoDB_Size\\tJoint_Calling_Verified\\tN_Expected\\tN_Imported\\tSample_List_md5sum" > ${wrk}/genoDB-Progress.txt
cat $(${soft}/software/bin/tree -fi Checks/ | grep "genoDB-data.txt" | sort -R | xargs) >> ${wrk}/genoDB-Progress.txt

less ${wrk}/genoDB-Progress.txt


# List crap GATK-GenomicsDB shards
awk '$5 != 1' ${wrk}/genoDB-Progress.txt
```

