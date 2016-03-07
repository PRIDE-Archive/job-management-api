package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model;

import uk.ac.ebi.interpro.metagenomics.jobManager.IJobOption;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: craigm
 * Date: 30/10/13
 * Time: 10:44
 * To change this template use File | Settings | File Templates.
 */
public enum LSFBkillOption implements IJobOption {

    JOB_NAME("-J", "job_name",true),
    PROJECT_NAME("-P", "project_name",true);

    private String shortOpt;

    private String description;

    private boolean argumentRequired;

    private LSFBkillOption(
            String shortOpt,
            String description,
            boolean argumentRequired
    ) {
        this.shortOpt = shortOpt;
        this.description = description;
        this.argumentRequired = argumentRequired;
    }

    public String getShortOpt() {
        return shortOpt;
    }

    public boolean isArgumentRequired() {
        return argumentRequired;
    }

    public String getDescription() {
        return description;
    }

}
