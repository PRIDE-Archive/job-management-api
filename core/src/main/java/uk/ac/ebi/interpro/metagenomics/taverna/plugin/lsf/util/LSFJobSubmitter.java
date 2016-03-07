package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.util;

import org.apache.log4j.Logger;
import uk.ac.ebi.interpro.metagenomics.jobManager.GenericJobBatch;
import uk.ac.ebi.interpro.metagenomics.jobManager.IJobSubmitter;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.LSFBsubCommand;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.LSFBsubOption;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.LSFJob;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;

/**
 *
 *
 * @author Maxim Scheremetjew, EMBL-EBI, InterPro
 * @version $Id: LSFJobSubmitter.java,v 1.2 2013/11/26 15:35:43 matthew Exp $
 * @since 1.0-SNAPSHOT
 */
public class LSFJobSubmitter implements IJobSubmitter<LSFJob> {

    private static final Logger LOGGER = Logger.getLogger(LSFJobSubmitter.class.getName());

    private static final LSFJobSubmitter instance = new LSFJobSubmitter();

    private LSFJobSubmitter() {
    }

    public static LSFJobSubmitter getInstance() {
        return instance;
    }


    /**
     * Use this method to submit a batch of depended jobs with the same project name you would like to monitory at the same time.
     *
     * @param lsfJobBatch Batch of jobs. Job batches can contain 1 to n jobs.
     */
    //TODO: Submit many jobs within a single process (if necessary!). Speak to systems about that.
    public void submitBatch(final GenericJobBatch<LSFJob> lsfJobBatch) {
        if (lsfJobBatch != null) {
            submitJobs(lsfJobBatch.getJobs());
        } else {
            LOGGER.warn("Specified batch of jobs is NULL.");
        }
    }

    /**
     * Use this method to submit multiple individual jobs.
     *
     * @param jobs
     */
    public void submitJobs(final Set<LSFJob> jobs) {
        if (jobs != null) {
            if (LOGGER.isInfoEnabled())
                LOGGER.info("Submitting a collection of " + jobs.size() + " jobs...");
            for (LSFJob job : jobs) {
                submitJob(job);
            }
        } else {
            LOGGER.warn("Specified collection of jobs is NULL.");
        }
    }


    /**
     * Use this method to submit single job.
     *
     * @param job
     */
    public void submitJob(final LSFJob job) {
        if (job != null) {
            LSFBsubCommand lsfCommand = job.getJobSchedulerCommand();
            String jobName = (String) job.getOption(LSFBsubOption.JOB_NAME);
            if (jobName != null) {
                if (LOGGER.isDebugEnabled())
                    LOGGER.debug("Starting job submission of job with name " + jobName);
            }
            List<String> command = lsfCommand.getCommand();
            if (command != null && command.size() > 0) {
                if (LOGGER.isDebugEnabled())
                    LOGGER.debug("Running the following command: " + command);

                ProcessBuilder pb = new ProcessBuilder(command);
                int exitStatus;
                try {
                    Process process = pb.start();
                    exitStatus = process.waitFor();

                    if (exitStatus == 0) {
                        LOGGER.debug("Job submission finished successfully with exit code 0!");
                    } else {
                        InputStream is = process.getInputStream();

                        StringBuffer failureMessage = new StringBuffer();
                        failureMessage.append("Bsub command line failed with exit code: ")
                                .append(exitStatus)
                                .append("\nBsub command: ");
                        for (String element : command) {
                            failureMessage.append(element).append(' ');
                        }
                        failureMessage.append("Bsub command output:\n").append(Utils.getStandardOutput(is));
                        throw new IllegalStateException(failureMessage.toString());
                    }
                } catch (InterruptedException e) {
                    throw new IllegalStateException("InterruptedException thrown when attempting to run the lsf command", e);
                } catch (IOException e) {
                    LOGGER.error("Cannot get ", e);
                }

            } else {
                LOGGER.warn("The specified LSF job command is NULL or empty.");
            }
        } else {
            LOGGER.warn("Specified LSF job is NULL.");
        }
    }
}
