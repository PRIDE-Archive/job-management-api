package uk.ac.ebi.interpro.metagenomics.jobManager;

import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: craigm
 * Date: 15/10/13
 * Time: 16:54
 * To change this template use File | Settings | File Templates.
 */
public interface IJobKiller<T extends GenericJob> {

    public void killJob(T job) ;

    public void killJobs(Set<T> jobs);


}
