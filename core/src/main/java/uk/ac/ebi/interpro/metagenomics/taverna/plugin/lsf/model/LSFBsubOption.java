package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model;

import uk.ac.ebi.interpro.metagenomics.jobManager.IJobOption;

/**
 * Represents a valid LSF option.
 *
 * @author Maxim Scheremetjew, EMBL-EBI, InterPro
 * @version $Id: LSFBsubOption.java,v 1.4 2013/11/28 17:00:40 matthew Exp $
 * @since 1.0-SNAPSHOT
 */
public enum LSFBsubOption implements IJobOption {
    QUEUE_NAME("-q", "Name of the queue you like to submit", true),
    JOB_NAME("-J", "job_name", true),
    JOB_DESC("-Jd", "job_description", true),
    MIN_MAX_PROCESSORS("-n", "min_proc[,max_proc]", true),
    STD_OUTPUT_FILE("-o", "output_file", true),
    STD_ERROR_OUTPUT_FILE("-e", "error_file", true),
    MAX_MEMORY("-M", "memory limit", true),
    RESOURCE_REQUIREMENT("-R", "resource requirement, e.g. for memory 'rusage[mem=6000]'", true),
    PROJECT_NAME("-P", "project_name", true);

    private String shortOpt;

    private String description;

    private boolean argumentRequired;

    private LSFBsubOption(
            String shortOpt,
            String description,
            boolean argumentRequired
    ) {
        this.shortOpt = shortOpt;
        this.description = description;
        this.argumentRequired=argumentRequired;
    }

    public String getShortOpt() {
        return shortOpt;
    }

    public String getDescription() {
        return description;
    }

    public boolean isArgumentRequired() {
        return argumentRequired;
    }
}
