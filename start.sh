#!/bin/bash

# Set variables from softdrive
soft=/cvmfs/softdrive.nl/projectmine_sw


# Set variables for reference data
b37=${soft}/resources/Build37
ref=${b37}/hs37d5.fa
refHG19=/cvmfs/softdrive.nl/projectmine_sw/resources/Build37/hg19/ucsc.hg19.fasta


# Set build 38 reference data
b38=${soft}/resources/Build38/hs38DH
ref38=${b38}/hs38DH.fa
dbSNP38=${b38}/dbsnp_146.hg38.vcf.gz
tgt=${b38}/cds_100bpSplice_utr_codon_mirbase.bed
OMNI=${b38}/1000G_omni2.5.hg38.vcf.gz
dbSNP=${b38}/dbsnp_146.hg38.vcf.gz
MILLS=${b38}/Mills_and_1000G_gold_standard.indels.hg38.vcf.gz
HAPMAP=${b38}/hapmap_3.3.hg38.vcf.gz
KGgold=${b38}/1000G_phase1.snps.high_confidence.hg38.vcf.gz


# Alignment software
gatk4=${soft}/bin/gatk-4.1.4.0/gatk-package-4.1.4.0-local.jar
BWA=${soft}/bin/bwa
SAMTOOLS=${soft}/bin/samtools
TABIX=${soft}/bin/tabix
SAMBLASTER=${soft}/samblaster/samblaster
BCFTOOLS=${soft}/bin/bcftools


# Set dCache paths
MinE_Tape=gsiftp://gridftp.grid.sara.nl/pnfs/grid.sara.nl/data/lsgrid/Project_MinE/Tape
MinE_Disk=gsiftp://gridftp.grid.sara.nl/pnfs/grid.sara.nl/data/lsgrid/Project_MinE/Disk
SRM_Disk=srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/lsgrid/Project_MinE/Disk
SRM_Tape=srm://srm.grid.sara.nl/pnfs/grid.sara.nl/data/lsgrid/Project_MinE/Tape
project=${MinE_Disk}/User/${USER}/projects
projectTape=${MinE_Tape}/User/${USER}/projects
projectSRM=${SRM_Disk}/User/${USER}/projects
projectTapeSRM=${SRM_Tape}/User/${USER}/projects


# Set bam level result path
bams=${projectTape}/process_bam
alignmentDir=${bams}/alignment
bamDir=${bams}/bam
gvcf=${bams}/gvcf
