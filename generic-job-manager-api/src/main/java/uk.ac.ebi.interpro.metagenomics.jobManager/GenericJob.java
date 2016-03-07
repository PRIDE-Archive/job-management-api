package uk.ac.ebi.interpro.metagenomics.jobManager;

import java.io.Serializable;

/**
 * Represents a generic job which can be submitted to a farm/cluster.
 *
 * @author Maxim Scheremetjew, EMBL-EBI, InterPro
 * @version $Id: GenericJob.java,v 1.6 2013/11/25 09:42:26 craigm Exp $
 * @since 1.0-SNAPSHOT
 */
public abstract class GenericJob<T extends GenericJobCommand> implements Serializable {

    private T jobSchedulerCommand;

    // allow serialization
    protected GenericJob() {

    }

    protected GenericJob(T jobSchedulerCommand) {
        this.jobSchedulerCommand = jobSchedulerCommand;
    }

    public T getJobSchedulerCommand() {
        return jobSchedulerCommand;
    }

    public Object getOption(IJobOption optionKey) {
        return jobSchedulerCommand.getOptionValue(optionKey);

    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GenericJob)) return false;

        GenericJob that = (GenericJob) o;

        if (!jobSchedulerCommand.equals(that.jobSchedulerCommand)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return jobSchedulerCommand.hashCode();
    }
}