To Do:

```
a). Testing & job submission:

	1). Sanity check scripts with NEK1 + SOD1 aligments:
		=> Data writing to "Test_Samples" in disk NL
	
	2). Main read me code blocks:

		i). Setting up (tokens on couchDB) and running jobs (gLite-wms).
			=> Assumes required files are staged.

		ii). Using task scripts as they are (i.e Alignment.sh, DedupBQSR.sh etc).
```



```
b). Job Management read me code blocks:

	i). Using the job management scripts for summarizing token states, restarting tokens and deleting tokens.
```



*FYI: Ignore the Joint Calling folder for now as some scripts are actively being worked on*. Progress described below & supporting sample information / data processing progress found in the  "*AWS-dbGaP-Grid*" google drive.

```
- Disclaimer: Proceed with caution ;)

- Added core scripts for grid & HPC (ie import a list of gVCFs)

- Added helper scripts for sanity checking each of the genomics DB for:
	a). Who has been / yet to be imported.
	b). Call rates for the imported samples per shard.
	c). Offloading onto the HPC for adding the final ~100 samples or if grid causes issues.
```

