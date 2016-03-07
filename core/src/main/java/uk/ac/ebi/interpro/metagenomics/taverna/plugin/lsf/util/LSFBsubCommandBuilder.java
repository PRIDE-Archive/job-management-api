package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.util;

import uk.ac.ebi.interpro.metagenomics.jobManager.SystemCommand;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.LSFBsubCommand;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.LSFBsubOption;

/**
 * Represents a LSF bsub command builder.
 * //TODO: Rethink if this is really necessary and helpful.
 *
 * @author Maxim Scheremetjew, EMBL-EBI, InterPro
 * @version $Id: LSFBsubCommandBuilder.java,v 1.7 2014/05/12 14:38:54 craigm Exp $
 * @since 1.0-SNAPSHOT
 */
public class LSFBsubCommandBuilder {

    private String queueName;

    private String jobName;

    private String stdOutputFile;

    private String stdErrOutputFile;

    private String projectName;

    private int minProcessors;

    private int maxProcessors;

    private int maxMemory;

    private String resourceRequirement;

    /**
     * String List representation of a system systemCmd, which will be submitted to the farm.
     */
    private SystemCommand systemCmd;

    public LSFBsubCommandBuilder(SystemCommand systemCmd) {
        if (systemCmd == null) {
            throw new IllegalStateException("System command cannot be NULL!");
        }
        this.systemCmd = systemCmd;
    }

    public LSFBsubCommandBuilder setQueueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    public LSFBsubCommandBuilder setJobName(String jobName) {
        this.jobName = jobName;
        return this;
    }

    public LSFBsubCommandBuilder setProjectName(String projectName) {
        this.projectName = projectName;
        return this;
    }

    public LSFBsubCommandBuilder setStdOutputFile(String stdOutputFile) {
        this.stdOutputFile = stdOutputFile;
        return this;
    }

    public LSFBsubCommandBuilder setStdErrOutputFile(String stdErrOutputFile) {
        this.stdErrOutputFile = stdErrOutputFile;
        return this;
    }

    public LSFBsubCommandBuilder setMinProcessors(int minProcessors) {
        this.minProcessors = minProcessors;
        return this;
    }

    public LSFBsubCommandBuilder setMaxProcessors(int maxProcessors) {
        this.maxProcessors = maxProcessors;
        return this;
    }

    public LSFBsubCommandBuilder setMaxMemory(int maxMemory) {
        this.maxMemory = maxMemory;
        return this;
    }

    public LSFBsubCommandBuilder setResourceRequirement(String resourceRequirement) {
        this.resourceRequirement = resourceRequirement;
        return this;
    }


    public LSFBsubCommand build() {
        LSFBsubCommand bsubCommand = new LSFBsubCommand(this.systemCmd);
        if (this.queueName != null && this.queueName.length() > 0) {
            bsubCommand.putCommandOption(LSFBsubOption.QUEUE_NAME, queueName);
        }
        if (this.jobName != null && this.jobName.length() > 0) {
            bsubCommand.putCommandOption(LSFBsubOption.JOB_NAME, jobName);
        }
        if (this.stdOutputFile != null) {
            bsubCommand.putCommandOption(LSFBsubOption.STD_OUTPUT_FILE, stdOutputFile);
        }
        if (this.stdErrOutputFile != null) {
            bsubCommand.putCommandOption(LSFBsubOption.STD_ERROR_OUTPUT_FILE, stdErrOutputFile);
        }
        if (this.maxProcessors != 0) {
            if (this.minProcessors != 0)
                bsubCommand.putCommandOption(LSFBsubOption.MIN_MAX_PROCESSORS, minProcessors + "," + maxProcessors);
            else
                bsubCommand.putCommandOption(LSFBsubOption.MIN_MAX_PROCESSORS, maxProcessors);
        }
        if (this.maxMemory != 0) {
            bsubCommand.putCommandOption(LSFBsubOption.MAX_MEMORY, maxMemory);
        }
        if (this.resourceRequirement != null) {
            bsubCommand.putCommandOption(LSFBsubOption.RESOURCE_REQUIREMENT, resourceRequirement);
        }
        return bsubCommand;
    }
}
