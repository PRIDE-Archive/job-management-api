package uk.ac.ebi.interpro.metagenomics.jobManager;

import java.util.Set;

/**
 * Represents a generic job submitter.
 *
 * @author Maxim Scheremetjew, EMBL-EBI, InterPro
 * @version $Id: IJobSubmitter.java,v 1.3 2013/11/26 15:35:43 matthew Exp $
 * @since 1.0-SNAPSHOT
 */
public interface IJobSubmitter<T extends GenericJob> {

    /**
     * Submits a batch of jobs
     */
    public void submitBatch(GenericJobBatch<T> jobBatch);

    /**
     * Submits single
     */
    public void submitJob(T job);

    /**
     * Submits a collection of jobs
     */
    public void submitJobs(Set<T> jobs);
}
