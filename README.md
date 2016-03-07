# job-management-api
Generic Java interfaces for managing jobs on computer cluster like LSF

## 1. Introduction ##

Job submission and monitoring should be simple. This is the main motivation behind this Java API. One part of this project is a generic module which could used
and expanded for any cluster implementation.
The second part of this project is a LSF implementation using the interfaces of the generic module.

## 2. LSF job submission and monitoring ##

A good starting point to use this API is the LSFManager class. The LSF manager is like a repository which gives you access to most of the functionality.

### 2.1 How to create a LSF job ###

At the moment there are 2 ways which allow you to create a LSF job.
#### 2.1 Using built-in 'LSFBsubCommandBuilder' ####

//Create a system command you would like to run
SystemCommand systemCommand = new SystemCommand(command);
//Create a bsub command using the 'LSFBsubCommandBuilder'
LSFBsubCommandBuilder bsubCommandBuilder = LSFManager.getLSFBsubCommandBuilder(systemCommand);
LSFBsubCommand bsubCommand = bsubCommandBuilder.
setJobName("jobName").
setQueueName("queueName").
setMaxProcessors(8).
setMinProcessors(2).
setStdOutputFile("test.out").
setStdErrOutputFile("test.err").
setMaxMemory(500).
setReserveMemory(true).
build();
//Finally create the LSF job
LSFJob lsfJob = new LSFJob(lsfCommand);

#### 2.2 Create LSF bsub command directly ####

//Create a system command you would like to run
SystemCommand systemCommand = new SystemCommand(command);
//Create a bsub command directly
LSFBsubCommand lsfBsubCommand = new LSFBsubCommand(systemCommand);
lsfBsubCommand.addCommandOption(LSFBsubOption.QUEUE_NAME, "queueName");
lsfBsubCommand.addCommandOption(LSFBsubOption.JOB_NAME, "jobName");
lsfBsubCommand.addCommandOption(LSFBsubOption.STD_OUTPUT_FILE, "test.out");
lsfBsubCommand.addCommandOption(LSFBsubOption.STD_ERROR_OUTPUT_FILE, "test.err");
lsfBsubCommand.addCommandOption(LSFBsubOption.MIN_MAX_PROCESSORS, "2,8");
lsfBsubCommand.addCommandOption(LSFBsubOption.MAX_MEMORY, 500);
lsfBsubCommand.addCommandOption(LSFBsubOption.RESERVE_MEMORY, true);
//Finally create the LSF job
LSFJob lsfJob = new LSFJob(lsfCommand);

## 3. How to submit individual jobs ##

//...
//Create a LSF job instance like described above
LSFJob lsfJob = new LSFJob(bsubCommand);
//Get the job submitter from the manager
LSFJobSubmitter jobSubmitter = LSFManager.getLSFJobSubmitter();
//Submit the job
jobSubmitter.submitJob(lsfJob);

## 4. How to submit multiple jobs ##

//...
//Create a collection of LSF jobs
Collection<LSFJob> jobs = ...;
//Get the job submitter from the manager
LSFJobSubmitter jobSubmitter = LSFManager.getLSFJobSubmitter();
//Submit the job
jobSubmitter.submitJobs(jobs);

## 5. Job monitoring ##

This API allows you to monitor batches of jobs. A batch can consist of 1 or multiple jobs. The submission process for job batches is a bit different then described in chapter 3 and 4. First of all you have to create a batch of jobs which you will then submit and monitor.
### 5.1 How to create a batch of jobs ###

//Create a collection of batch items
Collection<LSFBatchItem> batchItems = &;
//Create a LSF batch of jobs using the batch factory
LSFJobBatchFactory batchFactory = LSFManager.getLSFJobBatchFactory();
LSFJobBatch jobBatch = batchFactory.createLSFBatch(batchItems);
//Submit the LSF batch to the cluster
LSFJobSubmitter jobSubmitter = LSFManager.getLSFJobSubmitter();
jobSubmitter.submitBatch(jobBatch);

### 5.2 How to monitor a batch ###

The monitorBatch(...) method from the LSFBatchMonitor class returns a boolean flag, which indicates if all jobs of the batch finished successfully or not. (It might be that we need to expand that boolean flag to a monitor result object which gives you more useful information about how many jobs finished unsuccessfully and why)
//...
LSFJobBatch jobBatch = &;
//Monitor the LSF batch
LSFBatchMonitor monitor = LSFManager.getLSFBatchMonitor();
boolean monitorResult = monitor.monitorBatch(jobBatch);

