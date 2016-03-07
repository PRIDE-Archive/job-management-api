package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.util;

import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.statistics.LSFJobBatchStatisticsSummary;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.io.LSFJobInfoParser;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.io.LSFStdOutputParser;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.statistics.LSFJobStatistics;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.management.LSFManager;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.*;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.*;

/**
 * Can be used to monitor a single LSF job batch. Resubmits failed jobs if required.
 * <p/>
 * !!!Please notice, this class isn't thread safe and can only be use for a single batch.
 *
 * @author Maxim Scheremetjew, EMBL-EBI, InterPro
 * @author craigm
 * @author Matthew Fraser
 * @version $Id: LSFBatchMonitor.java,v 1.26 2014/01/15 17:00:06 maxim Exp $
 * @since 1.0-SNAPSHOT
 */
//TODO: Do we need something like a monitor result to get the intermediate status?
public class LSFBatchMonitor {

    private static final Logger LOGGER = Logger.getLogger(LSFBatchMonitor.class.getName());
    private final List<LSFJob> uncompletedJobs = new ArrayList<LSFJob>();
    private final List<LSFJob> successfulJobs = new ArrayList<LSFJob>();
    private final List<LSFJobStatistics> successfulJobsStatistics = new ArrayList<LSFJobStatistics>();
    private final Map<LSFJob, Integer> failedJobs = new HashMap<LSFJob, Integer>();
    private final Map<LSFJob, Integer> indeterminateJobs = new HashMap<LSFJob, Integer>();
    private List<LSFJobStatistics> outputStatistics = new ArrayList<LSFJobStatistics>();

    /*
      Some statuses returned by LSF mean that the precise status of the job cannot be determined
      If a job has ZOMBI or UNKWN status then the job may have failed due to problems on the host,
      or it might be that the host was too busy to respond to the LSF query about the job.
      On the advice of systems, jobs with these statuses should only be assumed to have failed if
      this status is consistently observed after several status queries
    */
    private static final Set<LSFJobStatus> INDETERMINATE_STATUSES = new HashSet<LSFJobStatus>();

    private static final int MAX_QUERIES_FOR_INDETERMINATE_JOBS = 5;

    static {
        INDETERMINATE_STATUSES.add(LSFJobStatus.UNKWN);
        INDETERMINATE_STATUSES.add(LSFJobStatus.ZOMBI);
    }

    //statuses that indicate a job is still active within lsf
    private static final Set<LSFJobStatus> ACTIVE_STATUSES = new HashSet<LSFJobStatus>();

    static {
        ACTIVE_STATUSES.add(LSFJobStatus.RUN);
        ACTIVE_STATUSES.add(LSFJobStatus.PEND);
        ACTIVE_STATUSES.add(LSFJobStatus.PSUSP);
        ACTIVE_STATUSES.add(LSFJobStatus.SSUSP);
        ACTIVE_STATUSES.add(LSFJobStatus.USUSP);
    }


    private int lsfCheckFrequencyMillis = 5 * 60 * 1000;  // default 5 minutes


    public LSFJobBatchStatisticsSummary monitorBatch(LSFJobBatch batch) {
        return monitorBatch(batch, new LSFResubmissionStrategy(), lsfCheckFrequencyMillis);
    }

    public LSFJobBatchStatisticsSummary monitorBatch(LSFJobBatch batch, LSFResubmissionStrategy resubmissionStrategy) {
        return monitorBatch(batch, resubmissionStrategy, lsfCheckFrequencyMillis);

    }

    /**
     * This methods blocks until the lsf batch can be defined as successful or failed
     *
     * @param batch
     * @param resubmissionStrategy
     * @param waitingTime In milli seconds.
     * @return
     */
    public LSFJobBatchStatisticsSummary monitorBatch(LSFJobBatch batch,  LSFResubmissionStrategy resubmissionStrategy, int waitingTime) {
        boolean isSpeculative = batch instanceof LSFSpeculativeJobBatch;
        LSFSpeculativeMonitor specMonitor = new LSFSpeculativeMonitor();

        // safety measure - if we are restarting after a failure there could be speculative jobs running
        // to ensure we don't start two identical speculative jobs then we kill any currently running
        // TODO: handle this possibility in the speculative monitor

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Monitoring batch with project name " + batch.getProjectName());
        }
        uncompletedJobs.addAll(batch.getJobs());
        int batchSize = batch.getBatchSize();
        //Build bjobs command
        LSFBjobsCommand monitorCmd = buildBjobsCommand(batch.getProjectName());
        LSFJobInfoParser parser = LSFManager.getLSFJobInfoParser();
        int maxAllowedFailures = resubmissionStrategy.getMaxFailures();
        // 10 second delay before monitoring in case there is a delay between
        // the job being submitted and showing up in the 'bjobs' command
        // TODO: discuss this with systems
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            LOGGER.error("LSFBatchMonitor thread interrupted while sleeping!", e);
        }

        while (!batchIsFinished(maxAllowedFailures)) {
            InputStream bjobsOutput = runBjobs(monitorCmd);
            Map<String, Set<LSFJobInformation>> jobInformationMap = parser.parse(bjobsOutput);
            if (jobInformationMap.size() > batchSize) {
                throw new IllegalStateException("The number of monitored jobs within LSF is greater than the total number of jobs in the batch. Possible project name collision.");
            }
            List<LSFJob> jobsToResubmit = checkUncompletedJobs(jobInformationMap);
            resubmitFailedJobs(jobsToResubmit, resubmissionStrategy);

            if (!batchIsFinished(maxAllowedFailures)) {
                // when the normal lsf jobs have been dealt with, check if speculative execution is needed
                if (isSpeculative) {
                    specMonitor.monitor((LSFSpeculativeJobBatch) batch);
                }

                try {
                    Thread.sleep(waitingTime);
                } catch (InterruptedException e) {
                    LOGGER.error("LSFBatchMonitor thread interrupted while sleeping!", e);
                }
            }
        }

        // once monitoring is finished, kill any speculative jobs (for safety reasons)
        if (isSpeculative) {
            specMonitor.killSpeculativeJobs();
        }

        if (batchFinishedSuccessfully()) {
            LOGGER.info("Finished monitoring. All jobs completed.");

        }
        if (successfulJobsStatistics.size() == successfulJobs.size()) {
            LOGGER.info("Collecting LSF statistics...");
            return new LSFJobBatchStatisticsSummary(successfulJobsStatistics);
        }  else throw new IllegalStateException("Problem encountered.\nThere are " + successfulJobs.size() +
                "successful jobs, but statistics for " + successfulJobsStatistics.size());

    }


    private boolean batchFinishedSuccessfully() {
        return uncompletedJobs.isEmpty() && failedJobs.isEmpty();
    }


    private void resubmitFailedJobs(List<LSFJob> jobsToResubmit, LSFResubmissionStrategy lsfResubmissionStrategy) {
        LSFJobSubmitter jobSubmitter = LSFManager.getLSFJobSubmitter();
        int maxAllowedFailures = lsfResubmissionStrategy.getMaxFailures();
        boolean alterMemoryForResubmission = lsfResubmissionStrategy.alterMemoryForResubmission();
        for (LSFJob lsfJob : jobsToResubmit) {
            if (failedJobs.get(lsfJob) < maxAllowedFailures) {
                LOGGER.info("Resubmitting a failed job");
                // the job has not reached its maximum allowed number of failures, so resubmit
                if (alterMemoryForResubmission) {
                    // if the memory needs altering then build a new command with the higher memory and submit it
                    // NB the job id and the project name will still be the same as before, so it can still be
                    // tracked within the batch
                    LSFBsubCommand command = lsfJob.getJobSchedulerCommand();
                    command.putCommandOption(LSFBsubOption.MAX_MEMORY, lsfResubmissionStrategy.getResubmissionMemory());
                    LSFJob alteredMemoryJob = new LSFJob(command);
                    jobSubmitter.submitJob(alteredMemoryJob);
                } else {
                    jobSubmitter.submitJob(lsfJob);
                }
            }  // TODO Consider logging jobs with too many failures here
        }
    }

    private List<LSFJob> checkUncompletedJobs(Map<String, Set<LSFJobInformation>> jobInformationMap) {
        List<LSFJob> jobsToResubmit = new ArrayList<LSFJob>();
        LSFStdOutputParser lsfStdOutParser = new LSFStdOutputParser();

        for (LSFJob uncompletedJob : uncompletedJobs) {
            String jobName = (String) uncompletedJob.getOption(LSFBsubOption.JOB_NAME);
            Set<LSFJobStatus> jobStatuses = new HashSet<LSFJobStatus>();
            if (jobInformationMap.containsKey(jobName)) {
                // if the job is still in LSF then get the status information
                Set<LSFJobInformation> jobInformations = jobInformationMap.get(jobName);
                jobStatuses = getJobStatuses(jobInformations);
            }

            if (isActiveInLSF(jobStatuses)) {
                // if the job is not yet successful, but is still active within lsf
                indeterminateJobs.remove(uncompletedJob);
            } else {

                String lsfStdOutFile = (String) uncompletedJob.getOption(LSFBsubOption.STD_OUTPUT_FILE);
                LSFJobStatistics jobStatistics;
                // if the job is successful
                if (jobStatuses.contains(LSFJobStatus.DONE)) {
                    jobStatistics = lsfStdOutParser.getJobStdOutputStatistics(lsfStdOutFile);
                    successfulJobs.add(uncompletedJob);
                    successfulJobsStatistics.add(jobStatistics);
                    failedJobs.remove(uncompletedJob);
                    indeterminateJobs.remove(uncompletedJob);
                    // TODO Add suspended jobs?
                } else {
                    // there is no instance of the job running or pending in lsf
                    // Already checked the lsf std output file - job may be successful but completed too long ago to still be in lsf
                    // Parse the lsf output non-strictly (not all info may be there)
                    jobStatistics = lsfStdOutParser.getJobStdOutputStatistics(lsfStdOutFile, false);
                    boolean jobSuccessful = jobStatistics != null && jobStatistics.isOK();
                    if (jobSuccessful) {
                        successfulJobs.add(uncompletedJob);
                        successfulJobsStatistics.add(jobStatistics);
                        failedJobs.remove(uncompletedJob);
                        indeterminateJobs.remove(uncompletedJob);
                        outputStatistics.add(jobStatistics);
                    } else {
                        // the job is not currently active in LSF and is not successful,
                        // so we need to check if it has failed, or if it is in an indeterminate status
                        // ie we can't tell from the status if the job has failed or not
                        boolean failed = true;
                        // First check if job has an indeterminate status
                        // If so, we need to check how many times it has been queried
                        // and returned this status to work out if it has actually failed or not
                        for (LSFJobStatus jobStatus : jobStatuses) {
                            if (INDETERMINATE_STATUSES.contains(jobStatus)) {
                                Integer jobQueryAttempts = indeterminateJobs.get(uncompletedJob);
                                // if the job status has not been indeterminate before, then record it
                                // and don't mark the job as failed
                                if (jobQueryAttempts == null) {
                                    indeterminateJobs.put(uncompletedJob, 1);
                                    failed = false;
                                } else {
                                    // if the job has not been queried the maximum number of times
                                    // then update how many times it has been queried and ensure it is
                                    // not marked as failed
                                    // TODO bkill the job (just to make sure)
                                    if (jobQueryAttempts < MAX_QUERIES_FOR_INDETERMINATE_JOBS) {
                                        indeterminateJobs.put(uncompletedJob, jobQueryAttempts + 1);
                                        failed = false;

                                    } else {
                                        // The job has been in an indeterminate state for the maximum allowed queries
                                        // and therefore is automatically marked as failed
                                        // As the job will be resubmitted, we remove it from the record
                                        // to give the resubmitted job a chance to succeed
                                        indeterminateJobs.remove(uncompletedJob);
                                    }
                                }
                            }
                        }
                        if (failed) {
                            jobsToResubmit.add(uncompletedJob);
                            // the job has failed, store it with the total number of times it has failed so far
                            if (failedJobs.containsKey(uncompletedJob)) {
                                int failures = failedJobs.get(uncompletedJob);
                                failedJobs.put(uncompletedJob, failures + 1);
                            } else {
                                failedJobs.put(uncompletedJob, 1);
                            }
                        }
                    }
                }
            }

        }

        // remove any successfully completed jobs from the uncompleted jobs collection
        uncompletedJobs.removeAll(successfulJobs);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("########## ...");
            LOGGER.info("########## " + uncompletedJobs.size() + " uncompleted jobs.");
            LOGGER.info("########## " + successfulJobs.size() + " successful jobs.");
            LOGGER.info("########## statistics for " + successfulJobsStatistics.size() + " successful jobs.");
            LOGGER.info("########## " + failedJobs.size() + " failed jobs.");
        }
        return jobsToResubmit;
    }

    private Set<LSFJobStatus> getJobStatuses(Set<LSFJobInformation> jobInformations) {
        Set<LSFJobStatus> jobStatuses = new HashSet<LSFJobStatus>();
        for (LSFJobInformation jobInformation : jobInformations) {
            LSFJobStatus jobStatus = jobInformation.getStatus();
            jobStatuses.add(jobStatus);
        }
        return jobStatuses;
    }

    private boolean isActiveInLSF(Set<LSFJobStatus> jobStatuses) {
        Set<LSFJobStatus> statuses = new HashSet<LSFJobStatus>(jobStatuses);
        statuses.retainAll(ACTIVE_STATUSES);
        return statuses.size() > 0;

    }


    protected LSFBjobsCommand buildBjobsCommand(String projectName) {
        Map<LSFBjobsOption, Object> options = new HashMap<LSFBjobsOption, Object>();
        options.put(LSFBjobsOption.WIDE_FORMAT, "");
        options.put(LSFBjobsOption.SHOW_ALL_STATES, "");
        options.put(LSFBjobsOption.PROJECT_NAME, projectName);
        return new LSFBjobsCommand(options);
    }

    private InputStream runBjobs(LSFBjobsCommand lsfCommand) {
        InputStream is = null;
        List<String> command = lsfCommand.getCommand();
        if (command != null && command.size() > 0) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Running the following command: " + command);
            }
            try {
                ProcessBuilder pb = new ProcessBuilder(command);
                Process pr = pb.start();
                int exitStatus = pr.waitFor();
                is = pr.getInputStream();
                if (exitStatus == 0) {
                    LOGGER.debug("Bjobs command finished successfully!");
                } else {
                    StringBuffer failureMessage = new StringBuffer();
                    failureMessage.append("Bjobs command failed with exit code: ")
                            .append(exitStatus)
                            .append("\nBjobs command: ");
                    for (String element : command) {
                        failureMessage.append(element).append(' ');
                    }
                    failureMessage.append("Bjobs output:\n").append(Utils.getStandardOutput(is));
                    throw new IllegalStateException(failureMessage.toString());
                }


            } catch (InterruptedException e) {
                throw new IllegalStateException("InterruptedException thrown when attempting to run binary", e);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return is;
    }

    private boolean maxAllowedFailuresReached(int maxAllowedFailures) {
        for (LSFJob lsfJob : failedJobs.keySet()) {
            if (failedJobs.get(lsfJob) >= maxAllowedFailures) {
                return true;
            }

        }
        return false;
    }


    private boolean batchIsFinished(int maxAllowedFailures) {
        if (uncompletedJobs.size() == 0) {
            return true;
        } else {
            return (maxAllowedFailuresReached(maxAllowedFailures));

        }

    }



    private class LSFSpeculativeMonitor {

        Map<LSFJob, LSFSpeculativeJob> runningSpeculativeJobs = new HashMap<LSFJob, LSFSpeculativeJob>();

        public void monitor(LSFSpeculativeJobBatch batch) {


            if ( (double)successfulJobs.size()/batch.getBatchSize() >= batch.getSpeculationStart()) {

                LSFJobKiller jobKiller = LSFJobKiller.getInstance();

                // check if speculative jobs need to be started
                if (runningSpeculativeJobs.size() == 0) {
                    for (LSFJob job: uncompletedJobs ) {
                        LSFSpeculativeJob specJob = (LSFSpeculativeJob) job;
                        runningSpeculativeJobs.put(specJob.getAlternateJob(), specJob);
                    }

                    LSFJobSubmitter submitter = LSFJobSubmitter.getInstance();

                    // before submitting any speculative jobs, we first run a bkill for these jobs
                    // the reason for this is that we may have restarted a batch and speculative
                    // jobs may bealready be  running.
                    // TODO implement a check for currently running speculative jobs
                    jobKiller.killJobs(runningSpeculativeJobs.keySet());
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("Starting " + runningSpeculativeJobs.size() + " speculative jobs.");
                    }
                    submitter.submitJobs(runningSpeculativeJobs.keySet());
                } else {
                    // remove any jobs that have completed successfully under normal execution
                    List<LSFJob> jobsToRemove = new ArrayList<LSFJob>();
                    for (LSFJob speculativeJob: runningSpeculativeJobs.keySet()) {
                        if (successfulJobs.contains(runningSpeculativeJobs.get(speculativeJob))) {
                            LOGGER.info("Removing speculative job that succeeded under normal execution");
                            jobsToRemove.add(speculativeJob);
                            jobKiller.killJob(speculativeJob);

                        }
                    }
                    // remove all dealt-with jobs
                    for (LSFJob job: jobsToRemove) {
                        runningSpeculativeJobs.remove(job);
                    }
                    jobsToRemove = new ArrayList<LSFJob>();
                    // deal with any successful speculative jobs
                    LSFBjobsCommand bjobsCommand = buildBjobsCommand(batch.getSpeculativeProjectName());
                    InputStream bjobsOutput = runBjobs(bjobsCommand);
                    LSFJobInfoParser parser = LSFManager.getLSFJobInfoParser();
                    Map<String, Set<LSFJobInformation>> jobInformationMap = parser.parse(bjobsOutput);

                    for (LSFJob job : runningSpeculativeJobs.keySet()) {
                        String jobName = (String) job.getOption(LSFBsubOption.JOB_NAME);
                        // if the job is still in LSF
                        if (jobInformationMap.containsKey(jobName)) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("Job " + jobName + " is in LSF");
                            }
                            Set<LSFJobInformation> jobInformationSet = jobInformationMap.get(jobName);
                            for (LSFJobInformation jobInformation : jobInformationSet) {
                                // if the job was successful then clean up
                                if (jobInformation.getStatus() == LSFJobStatus.DONE) {
                                    handleSuccessfulSpeculativeJob(runningSpeculativeJobs.get(job));
                                    jobsToRemove.add(job);
                                }
                            }
                        } else {
                            // the job is not mentioned in lsf at all
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("Job " + jobName + " is not in LSF");
                            }
                            String lsfStdOutputFile = (String) job.getOption(LSFBsubOption.STD_OUTPUT_FILE);
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug("Parsing LSF output for " + jobName + ": " + lsfStdOutputFile);
                            }
                            LSFStdOutputParser lsfStdOutParser = new LSFStdOutputParser();
                            LSFJobStatistics jobStats = lsfStdOutParser.getJobStdOutputStatistics(lsfStdOutputFile);
                            if (jobStats.isJobSuccessful()) {
                                handleSuccessfulSpeculativeJob(runningSpeculativeJobs.get(job));
                                jobsToRemove.add(job);
                            }

                            // NB currently do not handle failed speculative jobs
                        }
                    }
                    // remove all finished speculative jobs
                    for (LSFJob job: jobsToRemove) {
                        runningSpeculativeJobs.remove(job);
                    }
                }
            }
        }

        private void handleSuccessfulSpeculativeJob(LSFSpeculativeJob speculativeJob) {
            // to handle a successful speculative job we just kill the original and
            // move the files for the speculative job to make it seem as if the original was successful
            // then the main class will detect the job as succeeding
            LOGGER.info("Handling successful speculative job");
            LSFJobKiller jobKiller = LSFJobKiller.getInstance();
            // kill any other instance of the job
            LOGGER.debug("killing original job");
            jobKiller.killJob(speculativeJob);
            LOGGER.debug("Moving speculative output files");
            moveOutputFiles(speculativeJob);

        }


        private void moveOutputFiles(LSFSpeculativeJob speculativeJob) {
            File speculativeFile;
            File destinationFile;
            List<String> speculativeFilesToMove = speculativeJob.getSpeculativeFilesToMove();
            List<String> destinationFiles = speculativeJob.getDestinationFiles();
            for (int i = 0; i < speculativeJob.getSpeculativeFilesToMove().size(); i++){
                speculativeFile = new File(speculativeFilesToMove.get(i));
                destinationFile = new File(destinationFiles.get(i));
                try {
                    speculativeFile.renameTo(destinationFile);
                } catch (Exception e) {
                    throw new IllegalStateException(
                            "Could not move alternate output file: " + speculativeFile.getName() +
                                    " to destination: " + destinationFile.getName(), e);
                }

            }
        }

        private void killSpeculativeJobs() {
            LSFJobKiller lsfJobKiller = LSFManager.getLSFJobKiller();
            lsfJobKiller.killJobs(runningSpeculativeJobs.keySet());

        }

    }

}

