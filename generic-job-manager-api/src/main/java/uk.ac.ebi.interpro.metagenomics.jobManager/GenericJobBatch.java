package uk.ac.ebi.interpro.metagenomics.jobManager;

import java.io.Serializable;
import java.util.Set;

/**
 * Represents a generic batch of jobs.
 *
 * @author Maxim Scheremetjew, EMBL-EBI, InterPro
 * @version $Id: GenericJobBatch.java,v 1.4 2013/11/26 15:35:43 matthew Exp $
 * @since 1.0-SNAPSHOT
 */
public abstract class GenericJobBatch<T extends GenericJob> implements Serializable{

    private Set<T> jobs;

    // no-arg constructor to allow subclasses to be serialized
    protected GenericJobBatch() {} ;



    protected GenericJobBatch(Set<T> jobs) {
        this.jobs = jobs;
    }

    public Set<T> getJobs() {
        return jobs;
    }


}
