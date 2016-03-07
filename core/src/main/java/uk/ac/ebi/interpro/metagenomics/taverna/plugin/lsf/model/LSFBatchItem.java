package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model;

import uk.ac.ebi.interpro.metagenomics.jobManager.SystemCommand;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: craigm
 * Date: 10/08/12
 * Time: 09:27
 * To change this template use File | Settings | File Templates.
 */
public class LSFBatchItem {

    //mandatory
    private SystemCommand systemCommand;

    //mandatory
    private Map<LSFBsubOption, Object> options;

    public LSFBatchItem(SystemCommand systemCommand, Map<LSFBsubOption, Object> options) {
        checkOptionsAreValid(options);
        this.systemCommand = systemCommand;
        this.options = options;
    }

    public SystemCommand getSystemCommand() {
        return this.systemCommand;
    }

    public Map<LSFBsubOption, Object> getOptions() {
        return this.options;
    }

    private void checkOptionsAreValid(Map<LSFBsubOption, Object> options) {
        if (options == null || !options.containsKey(LSFBsubOption.STD_OUTPUT_FILE)) {
            throw new IllegalArgumentException("Must set the LSF standard output option for batch items");
        } else if (options.containsKey(LSFBsubOption.PROJECT_NAME)) {
            throw new IllegalArgumentException("Must not manually set the project name for batch items");
        } else if (options.containsKey(LSFBsubOption.JOB_NAME)) {
            throw new IllegalArgumentException("Must not manually set the job name for batch items");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LSFBatchItem that = (LSFBatchItem) o;

        if (!options.equals(that.options)) return false;
        if (!systemCommand.equals(that.systemCommand)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = systemCommand.hashCode();
        result = 31 * result + options.hashCode();
        return result;
    }
}
