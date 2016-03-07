package uk.ac.ebi.interpro.metagenomics.jobManager;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a system command which you can run speculatively on a cluster
 * ie if the original job is taking too long then submit a copy and see which finishes first
 * These commands should only write to an output file (or files) and not change system state
 * eg write to a database
 */
public abstract class SpeculativeCommand extends SystemCommand{

    protected List<String> outputFiles;

    public SpeculativeCommand(List<String> command) {
        super(command);
        outputFiles = new ArrayList<String>();
    }

    public List<String> getOutputFiles() {
        return outputFiles;
    }

    public abstract SpeculativeCommand replaceOutputFiles(List<String> newOutputFiles);


}
