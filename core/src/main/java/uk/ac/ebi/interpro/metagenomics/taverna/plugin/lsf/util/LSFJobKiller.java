package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.util;

import org.apache.log4j.Logger;
import uk.ac.ebi.interpro.metagenomics.jobManager.IJobKiller;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.LSFBkillCommand;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.LSFBkillOption;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.LSFBsubOption;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.LSFJob;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: craigm
 * Date: 15/10/13
 * Time: 16:48
 * To change this template use File | Settings | File Templates.
 */
public class LSFJobKiller implements IJobKiller<LSFJob>{

    private static final Logger LOGGER =  Logger.getLogger(LSFJobKiller.class.getName());

    private static LSFJobKiller jobKiller = null;

    private LSFJobKiller() {

    }

    public static LSFJobKiller getInstance() {
        if (jobKiller == null) {
            jobKiller = new LSFJobKiller();
        }
        return jobKiller;
    }

    @Override
    public void killJob(LSFJob job) {
        if (job != null) {
            String jobName = (String) job.getOption(LSFBsubOption.JOB_NAME);
            LOGGER.debug("Killing job with jobname: " + jobName);
            Map<LSFBkillOption, Object> map = new HashMap<LSFBkillOption, Object>();
            map.put(LSFBkillOption.JOB_NAME, jobName);
            LSFBkillCommand bkillCommand = new LSFBkillCommand(map);
            runBkillCommand(bkillCommand);

        } else {
            LOGGER.warn("Specified LSF job is NULL.");
        }

    }

    @Override
    public void killJobs(Set<LSFJob> jobs) {
        if (jobs != null) {
            for (LSFJob job: jobs) {
                killJob(job);
            }
        }
    }



    private void runBkillCommand(LSFBkillCommand bkillCommand) {
        ProcessBuilder pb = new ProcessBuilder(bkillCommand.getCommand());
        int exitStatus;
        try {
            Process process = pb.start();
            exitStatus = process.waitFor();

            if (exitStatus == 0) {
                LOGGER.debug("Job successfully killed");
            } else {
                InputStream is = process.getInputStream();
                String processOutput = Utils.getStandardOutput(is);
                if (processOutput.contains("No matching job found")) {
                    LOGGER.debug("Job is already dead!");
                } else {
                    StringBuffer failureMessage = new StringBuffer();
                    failureMessage.append("Bkill command failed with exit code: ")
                            .append(exitStatus)
                            .append("\nBkill command:\n")
                            .append(bkillCommand)
                            .append("\nProcess output:\n")
                            .append(processOutput)
                            .append("\nJob may have already stopped running");
                    LOGGER.debug(failureMessage.toString());
                }
            }

        } catch (InterruptedException e) {
            throw new IllegalStateException("Interrupted while trying to run bkill command: "
                    + bkillCommand, e);
        }  catch (IOException e) {
            LOGGER.warn("Cannot get " + e);
        }

    }

}
