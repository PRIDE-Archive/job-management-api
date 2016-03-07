package uk.ac.ebi.interpro.metagenomics.jobManager;

import java.io.Serializable;
import java.util.*;

/**
 * Represents generic job scheduler command for job submission, job monitoring etc.
 * <p/>
 * Synopsis
 * <p/>
 * job scheduler systemCmd [options] systemCmd [arguments]
 *
 * @author Maxim Scheremetjew, EMBL-EBI, InterPro
 * @version $Id: GenericJobCommand.java,v 1.8 2013/11/25 09:42:26 craigm Exp $
 * @since 1.0-SNAPSHOT
 */
public abstract class GenericJobCommand<T extends IJobOption> implements Serializable {

    private final String jobSchedulerCommand;

    private final Map<T, Object> options = new HashMap<T, Object>();

    // for serialization
    //protected GenericJobCommand() {}


    protected GenericJobCommand(String jobSchedulerCmd) {
        this(jobSchedulerCmd, null);
    }

    protected GenericJobCommand(String jobSchedulerCmd, Map<T, Object> options) {
        this.jobSchedulerCommand = jobSchedulerCmd;

        if (options != null) {
            this.options.putAll(options);
        }
    }

    public String getJobSchedulerCommand() {
        return jobSchedulerCommand;
    }


    public Map<T, Object> getOptions() {
        return options;
    }

    public Object getOptionValue(T optionKey) {
        return this.options.get(optionKey);
    }

    //TODO: Think if this fits the design strategy of immutable attributes
    public void putCommandOption(T key, Object value) {
        this.options.put(key, value);
    }

    //List representation of this command
    //TODO: make generic implementation (works for all except bsub!)
    public abstract List<String> getCommand();

    public abstract String toString();


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GenericJobCommand)) return false;

        GenericJobCommand that = (GenericJobCommand) o;

        if (jobSchedulerCommand != null ? !jobSchedulerCommand.equals(that.jobSchedulerCommand) : that.jobSchedulerCommand != null)
            return false;
        if (options != null ? !options.equals(that.options) : that.options != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = jobSchedulerCommand != null ? jobSchedulerCommand.hashCode() : 0;
        result = 31 * result + (options != null ? options.hashCode() : 0);
        return result;
    }
}