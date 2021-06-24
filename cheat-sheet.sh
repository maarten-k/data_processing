#!/bin/bash


######################################################################################
######################################################################################
# 
# Contents:
# 
# 	1. Core pipeline commands:
# 		a). Sequence aligment
# 		b). Downstream read processing (Dedup, BQSR)
# 		c). Variant Calling
# 		d). GATK-4 Genomics DB Importing
# 		e). Joint caling / GenotypeGVCF
# 
# 
# 	2. Grid cheat sheet (Maarten, Joke can help here):
# 
# 		NB ACTUAL DOCUMENTATION http://doc.grid.surfsara.nl/en/latest/index.html# 
# 
# 		a). Starting grid sessions
# 		b). Copying / Deleting & Staging Data
# 		c). Submitting jobs (gLite)
# 		d). Macaroons
#		e). PiCaS + couchDB:
# 
# 
######################################################################################
######################################################################################


#########################################################
#########################################################
# 
# 1. CORE PIPELINE COMMANDS
# 
#########################################################
#########################################################



########################################
########################################
# 
# Software & Resources
# 
########################################
########################################


# Store as variables
SOFT=/cvmfs/softdrive.nl/projectmine_sw
REF_DIR=${SOFT}/resources/Build38/hs38DH
TMPDIR=/scratch/${USER}
wrk=${TMPDIR}/${SAMPLE_ID}


# Store referece data paths
REF=${REF_DIR}/hs38DH.fa
dbSNP=${b38}/dbsnp_146.hg38.vcf.gz
Mills=${b38}/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz
KnownIndels=${b38}/Homo_sapiens_assembly38.known_indels.vcf.gz
BQSR_Loci=$(echo -L chr{1..22} | sed 's/ / -L /g' | sed 's/-L -L/-L/g')
EXOME_LOCI=${REF_DIR}/cds_100bpSplice_utr_codon_mirbase.bed
WGS_JOINT_CALLING_LOCI=${REF_DIR}/WGS_Loci/sub_shards
WGS_JOINT_CALLING_LOCI=${REF_DIR}/WXS
DB=/cvmfs/softdrive.nl/bkenna/Miniconda2/bin/job_management/meta.db
HG19=${SOFT}/resources/Build37/hg19/ucsc.hg19.fasta
GRCH37=/cvmfs/softdrive.nl/bkenna/Miniconda2/resources/Build37/hs37d5.fa


# Store software paths
export PATH="${SOFT}/software/bin:${PATH}"
SAMTOOLS=${SOFT}/software/bin/samtools
TABIX=${SOFT}/software/bin/tabix
BGZIP=${SOFT}/software/bin/bgzip
BCFTOOLS=${SOFT}/software/bin/bcftools
BWA=${SOFT}/software/bin/bwa
GATK4=${SOFT}/software/bin/gatk-4.1.4.0/gatk-package-4.1.4.0-local.jar
PICARD=/cvmfs/softdrive.nl/minegrp1/Miniconda2/bin/Picard_2_21/picard.jar
SAMBLASTER=/cvmfs/softdrive.nl/bkenna/Miniconda2/bin/samblaster/samblaster


# LASER Ancestry
LASER=${SOFT}/software/bin/LASER-2.04/laser
TRACE=${SOFT}/software/bin/LASER-2.04/trace
VCF_2_GENO=${SOFT}/software/bin/LASER-2.04/vcf2geno/vcf2geno


########################################
########################################
# 
# Commands from SOPs
# 
########################################
########################################


##############################
# 
# Extract & Align paired reads
# 
##############################


# Randomly shuffle, extract paired-end reads, align, sort and index to CRAM format
${SAMTOOLS} bamshuf -@ 8 --reference ${HG19} --output-fmt BAM -uOn 128 ${BAM} ${SAMPLE_ID}.tmp | ${SAMTOOLS} bam2fq -t -s /dev/null -1 ${SAMPLE_ID}.R1.fq.gz -2 ${SAMPLE_ID}.R2.fq.gz - > /dev/null


# Align, sort & cram compress
${BWA} mem -K 100000000 -t 8 -Y ${REF} -R "@RG\tID:${SAMPLE_ID}\tLB:${SAMPLE_ID}\tSM:${SAMPLE_ID}\tPL:ILLUMINA" ${SAMPLE_ID}.R1.fq.gz ${SAMPLE_ID}.R2.fq.gz 2>> ${SAMPLE_ID}.log | ${SAMBLASTER} -a --addMateTags | ${SAMTOOLS} view -h --threads 8 -bS > ${SAMPLE_ID}.aln.bam
rm -f ${SAMPLE_ID}.R1.fq.gz ${SAMPLE_ID}.R1.fq.gz
${SAMTOOLS} sort -@ 8 ${SAMPLE_ID}.aln.bam > ${SAMPLE_ID}.sorted.bam
rm -f ${SAMPLE_ID}.aln.bam
${SAMTOOLS} view -@ 8 -h -T ${REF} -C ${SAMPLE_ID}.sorted.bam > ${SAMPLE_ID}.sorted.cram
rm -f ${SAMPLE_ID}.sorted.bam
${SAMTOOLS} index ${SAMPLE_ID}.sorted.cram


##############################
# 
# Downstream read processing
# 
##############################


# Convert to bam, dedup and sort results
${SAMTOOLS} view -h -@ 8 -T ${REF} ${BAM} -b > ${SAMPLE_ID}.bam
${SAMTOOLS} index -@ 8 ${SAMPLE_ID}.bam
BAM=${SAMPLE_ID}.bam
rm -f ${SAMPLE_ID}*cram ${SAMPLE_ID}*cram.crai
java -Djava.io.tmpdir=${wrk} -jar ${PICARD} MarkDuplicates I=${BAM} AS=true O=${SAMPLE_ID}.dedup.bam METRICS_FILE=${SAMPLE_ID}.dedupMetrics.txt QUIET=true 2>> ${SAMPLE_ID}.dedup.log
rm -f ${BAM}*
${SAMTOOLS} sort -@ 8 -o ${SAMPLE_ID}.dedup-sorted.bam ${SAMPLE_ID}.dedup.bam
${SAMTOOLS} index -@ 8 ${SAMPLE_ID}.dedup-sorted.bam
rm -f ${SAMPLE_ID}.dedup.bam


# BQSR-GATK: Generate and apply their model
BAM=${SAMPLE_ID}.dedup-sorted.bam
java -Djava.io.tmpdir=${wrk} -jar ${GATK} -T BaseRecalibrator -I ${BAM} -R ${REF} -o ${SAMPLE_ID}.recal -nct 8 --downsample_to_fraction .1 ${BQSR_Loci} -knownSites ${dbSNP} -knownSites ${Mills} -knownSites ${KnownIndels} 2>> ${SAMPLE_ID}.bqsr.log

java -Djava.io.tmpdir=${wrk} -jar ${GATK} -T PrintReads -I ${BAM} -R ${REF} -nct 8 --BQSR ${SAMPLE_ID}.recal -o ${SAMPLE_ID}.bqsr.bam --globalQScorePrior -1.0 --preserve_qscores_less_than 6 --static_quantized_quals 10 --static_quantized_quals 20 --static_quantized_quals 30 --disable_indel_quals 2>> ${SAMPLE_ID}.bqsr.log
rm -f ${BAM}* ${SAMPLE_ID}.recal

${SAMTOOLS} sort -@ 8 ${SAMPLE_ID}.bqsr.bam -o ${SAMPLE_ID}.final.bam
rm -f ${SAMPLE_ID}.bqsr.bam*
${SAMTOOLS} view -h -@ 8 -T ${REF} -C ${SAMPLE_ID}.final.bam > ${SAMPLE_ID}.final-gatk.cram
${SAMTOOLS} index -@ 8 ${SAMPLE_ID}.final-gatk.cram
rm -f ${SAMPLE_ID}.final.bam*


##############################
# 
# Variant Calling
# 
##############################


# Call variants over exome (BAM or CRAM)
java -Djava.io.tmpdir=${wrk} -jar ${GATK} HaplotypeCaller -R ${REF} --dbsnp ${dbSNP} -I ${BAM} -O ${SAMPLE_ID}.g.vcf.gz -ERC GVCF -L ${EXOME_LOCI} --native-pair-hmm-threads 1 &>> ${wrk}/${SAMPLE_ID}.vcf.log


################
# 
# WGS Calling
# 
################


# Call variants over each chromosome
mkdir -p ${wrk}/chr1
cd ${wrk}/chr1
java -Djava.io.tmpdir=${wrk}/chr1 -jar ${GATK} HaplotypeCaller -R ${REF} --dbsnp ${dbSNP} -I ${wrk}/${BAM} -O ${wrk}/chr1/${SAMPLE_ID}.chr1.g.vcf.gz -L chr1 -ERC GVCF --native-pair-hmm-threads 1 &>> ${wrk}/${SAMPLE_ID}.vcf.log
zcat ${wrk}/chr1/${SM}.chr1.g.vcf.gz > ${wrk}/${SM}.g.vcf
rm -f ${wrk}/chr1/${SAMPLE_ID}.chr1.g.vcf.gz ${wrk}/chr1/${SAMPLE_ID}.chr1.g.vcf.gz.tbi


# Append chromsome calls
for chrom in chr{2..22} chr{X..Y}
	do
	mkdir -p ${wrk}/${chrom}
	cd ${wrk}/${chrom}
	java -Djava.io.tmpdir=${wrk}/${chrom} -jar ${GATK} HaplotypeCaller -R ${REF} --dbsnp ${dbSNP} -I ${wrk}/${BAM} -O ${wrk}/${chrom}/${SAMPLE_ID}.${chrom}.g.vcf.gz -L ${chrom} -ERC GVCF --native-pair-hmm-threads 1 &>> ${wrk}/${SAMPLE_ID}.vcf.log
	zcat ${wrk}/${chrom}/${SAMPLE_ID}.${chrom}.g.vcf.gz | grep -v "#" >> ${wrk}/${SAMPLE_ID}.g.vcf
	cd $wrk
	rm -fr ${chrom}
done


# Compress and index the WGS-gVCF
${BGZIP} -c ${wrk}/${SAMPLE_ID}.g.vcf > ${wrk}/${SAMPLE_ID}.g.vcf.gz
${TABIX} -p vcf -f ${wrk}/${SAMPLE_ID}.g.vcf.gz


##############################
# 
# Joint Calling
# 
##############################


# Set vars
ProjectID=MyDatabase
GATK_Loci_ARG=chrYYZ:1-750000
loci=1_750000
wrk=${TMPDIR}/${ProjectID}


################
# 
# genoDB import
# 
################


# Create workspace
echo -e "${SAMPLE_ID}\\t${GVCF_PATH}" >> ${wrk}/samples.sample_map
java -Djava.io.tmpdir=${wrk} -jar ${GATK} GenomicsDBImport --genomicsdb-workspace-path ${wrk}/genoDB/${ProjectID}-${loci} --batch-size 100 -L ${GATK_Loci_ARG} --sample-name-map ${wrk}/samples.sample_map --reader-threads 4 --consolidate &>> ${wrk}/${ProjectID}-${loci}.genoDB.log


# Update a workspace with new gVCFs
java -Djava.io.tmpdir=${wrk} -jar ${GATK} GenomicsDBImport --genomicsdb-update-workspace-path ${wrk}/genoDB/${ProjectID}-${loci} --batch-size 100 -L ${GATK_Loci_ARG} --sample-name-map ${wrk}/samples.sample_map --reader-threads 4 --consolidate &>> ${wrk}/${ProjectID}-${loci}.genoDB.log


#################
# 
# Query VCF
# 
#################


# Variable options for loci
tgt=chrYYZ:1-750000
tgt_2=${WXS_JOINT_CALLING_LOCI}/chr2/chr2.shard_123.bed


# Joint Calling / Generate Callset / Generate pVCF etc etc
java -Djava.io.tmpdir=${wrk} -jar ${GATK} GenotypeGVCFs -R ${REF} -O ${wrk}/${ProjectID}-${loci}.vcf.gz -D ${dbSNP38} -G StandardAnnotation -G AS_StandardAnnotation -V gendb://${wrk}/genoDB/${ProjectID} -L ${tgt} &>> ${wrk}/${ProjectID}-${loci}.Joint-Calling.log



#########################################################
#########################################################
# 
# 2. GRID CHEAT SHEET
# 
#########################################################
#########################################################



##############################
# 
# Interacting with Data
# 
##############################


# Start RCauth grid session
startGridSessionRCauth lsgrid:/lsgrid/Project_MinE


##############################
# 
# Managing data: SRM, GridFTP
# 
##############################


# Set convenient variables
MinE_ROOT=srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/lsgrid/Project_MinE
MinE_GLOBUS_ROOT=gsiftp://gridftp.grid.sara.nl/pnfs/grid.sara.nl/data/lsgrid/Project_MinE


# Stage a list of files for 2 weeks
toStage="${MinE_ROOT}/SomePath/SomeFile-1 ${MinE_ROOT}/SomePath/SomeFile-2 ${MinE_ROOT}/SomePath/SomeFile-3"
nohup srm-bring-online -lifetime=1209600 ${toStage} &>> nygc-staging.log &


# Copy files: Single, list
uberftp ${MinE_GLOBUS_ROOT}/SomePath/SomeFile-1 file://Some/Local/Path/SomeFile-1
echo -e "
${MinE_GLOBUS_ROOT}/SomePath/SomeFile-1 file://Some/Local/Path/SomeFile-1
${MinE_GLOBUS_ROOT}/SomePath/SomeFile-2 file://Some/Local/Path/SomeFile-2
" | awk 'NR > 1' > Transfers.txt
globus-url-copy -c -cd -concurrency 2 -f Transfers.txt


# Delete file
srmrm ${MinE_ROOT}/SomePath/SomeFile-1
uberftp -rm ${MinE_GLOBUS_ROOT}/SomePath/SomeFile-1



##############################
# 
# Submit Jobs: gLite
# 
##############################


# Write a JDL to submit parametric job / job array of 50 jobs
Njobs=50
View="Testing"
WallTime=3120
Ncores=2

echo -e '
JobType = "Parametric";
Parameters = '${Njobs}' ;
ParameterStart = 0 ;
ParameterStep = 1 ;
// Job Resources
Requirements = ( RegExp("gina",other.GlueCEUniqueID) && other.GlueCEPolicyMaxWallClockTime >= '${USER}' );
SMPgranularity = '${Ncores}';
CpuNumber = '${Ncores}';
ShallowRetryCount = 0;
RetryCount = 0;
// Executable
Executable = "/bin/bash";
Arguments = "/cvmfs/softdrive.nl/'${USER}'/Miniconda2/bin/Generic-JobScript.sh '${View}'";
// Standard Output and Error
StdOutput = "'${View}'-Parametric.txt";
StdError = "'${View}'-Parametric.txt";
// Output Sandbox Contents
OutputSandbox = { "'${View}'-Parametric.txt"};
' | awk 'NR > 1' > Some-Job.jdl


# Submit job
glite-wms-job-submit -d ${USER} -o Some-Job.txt Some-Job.jdl


# Cancel job
glite-wms-job-cancel https://wms2.grid.sara.nl:9000/kgfO0HClX40fWWa7WdCEAg
glite-wms-job-cancel -i Some-Job.txt --noint


# Check status
glite-wms-job-status https://wms2.grid.sara.nl:9000/kgfO0HClX40fWWa7WdCEAg
glite-wms-job-status -i Some-Job.txt --noint


# Get job logs
glite-wms-job-output https://wms2.grid.sara.nl:9000/kgfO0HClX40fWWa7WdCEAg
glite-wms-job-output -i Some-Job.txt --noint



##############################
# 
# PiCaS & Useful Links
# 	http://doc.grid.surfsara.nl/en/latest/Pages/Practices/picas/picas_overview.html
# 	http://doc.grid.surfsara.nl/en/latest/Pages/Practices/picas/picas_example.html
# 
##############################


###################
# 
# Create a design document to associate tokens to (akin to a table)
# 
# 
# 
####################


# PYTHON CODE: Method to allow summarizing tokens by their state: todo, locked, done
import couchdb
view = "Testing"
credentials.VIEW_NAME = view
db = couchdb.Database(credentials.URL + "/" + credentials.DBNAME)
db.resource.credentials = (credentials.USERNAME, credentials.PASS)
picasitems = {
 
    "language": "javascript",
    "views": {
        "overview": {
            "map": "function(doc) {\nif(doc[\"type\"]== \"token\"){\n\tif(doc[\"done\"] == 0 && doc[\"lock\"]==0  ){  emit(\"todo\",1);}\n\telse if(doc[\"done\"]== 0 && doc[\"lock\"]>= 1  ){ emit(\"locked\",1);}\n\telse if(doc[\"done\"]>= 1 && doc[\"lock\"]>= 1  ){ emit(\"done\",1);}\n\telse{emit(\"unknown_token_status\",1)}\n}\n\n}",
            "reduce": "function(key,values,rereduce){return sum(values);}"
        },
        "todo": {
            "map": "function(doc) {\nif(doc[\"type\"]== \"token\"){\n\tif(doc[\"done\"]==0 && doc[\"lock\"]==0  ){\n\t\tif(\"total_chunks\" in doc){\n\t\t\tif (doc[\"total_chunks\"]==doc[\"chunks_ready\"]){\n\t\t\t\temit(doc._id, doc._id);\n\t\t\t}\n\t\t}else{\n\t  \t\temit(doc._id, doc._id);\n\t\t}\n\t\n\t}\n}\n}"
        },
        "done": {
            "map": "function(doc) {\nif(doc[\"type\"]== \"token\"){\n\tif(doc[\"done\"]!= 0){\n  emit(doc[\"_id\"], doc[\"_id\"]);\n}\n}\n}"
        },
        "locked": {
            "map": "function(doc) {\n\tif(doc[\"type\"]== \"token\"){\n\t\tif(doc[\"done\"]== 0 && doc[\"lock\"]>= 1  ){ \n  \t\t\temit(doc[\"_id\"],doc);\n\t\t}\n\t}\n}"
        },
	"error": {
           "map": "function(doc) {\n\tif(doc[\"type\"]==\"token\"){if (doc[\"lock\"]==-1 && doc[\"done\"]==-1) {\n\t\t\temit(doc[\"_id\"], doc);\n\t\t}\n\t}\n}"
       }
    }
}


# Put the design document into database
db["_design/" + stage] = picas_items



#############################
# 
# Create token
# 
#############################


# PYTHON: Connect to couchDB
import couchdb, credentials, random
view = "Testing"
credentials.VIEW_NAME = view
db = couchdb.Database(credentials.URL + "/" + credentials.DBNAME)
db.resource.credentials = (credentials.USERNAME, credentials.PASS)


# PYTHON: Create 10 tokens
for i in range(0,9):
	token_name = str( "SuperAwesome_Token_" + str(round(random.random() * 200)) + "_Testing")
	taskScript = str("bash ${soft}/BWA-Alignment.sh " + i)
	if tokenname in db:
		print("Skipping " + tokenname)
	else:
		token = {
			"_id": token_name,
			"lock": 0,
			"done": 0,
			"type": view,
			"files": 'doggie',
			"Task_ID": i,
			"Task_Script": taskScript
		}
		db.save(token)


#############################
# 
# Run application:
#  Iterate over todo tokens of design document
# 
#############################


# SHELL: Run a generic PiCaS application to execute the task script of each token
# The templated variable in task script is defined as global variable in "Generic-Job-Script.sh" that execute this application
python PiCaS-General.py "Testing"

