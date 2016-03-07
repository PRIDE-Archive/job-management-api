package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model;

import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: craigm
 * Date: 16/10/13
 * Time: 09:54
 * To change this template use File | Settings | File Templates.
 */
public class LSFSpeculativeJobBatch extends LSFJobBatch {

    // proportion of the batch that must complete before speculative jobs are started
    public static final double DEFAULT_SPECULATION_START = 0.9;
    private final double speculationStart;
    private final String speculativeProjectName;


    public LSFSpeculativeJobBatch(Set<LSFSpeculativeJob> jobs, String projectName) {
        this(jobs, projectName, DEFAULT_SPECULATION_START);
    }

    public LSFSpeculativeJobBatch(Set<LSFSpeculativeJob> jobs, String projectName, double speculationStart) {
        super(jobs, projectName);
        if (speculationStart < 0.7) {
            throw new IllegalArgumentException("Cannot start speculation if job batch is less than 70% complete");
        }
        this.speculationStart = speculationStart;
        LSFSpeculativeJob job = (LSFSpeculativeJob)jobs.toArray()[0];
        speculativeProjectName = job.getSpeculativeProjectName();


    }

    public double getSpeculationStart() {
        return this.speculationStart;
    }

    public String getSpeculativeProjectName() {
        return speculativeProjectName;
    }

}
