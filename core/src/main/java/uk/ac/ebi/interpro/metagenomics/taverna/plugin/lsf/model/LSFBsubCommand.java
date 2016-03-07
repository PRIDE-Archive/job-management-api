package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model;

import uk.ac.ebi.interpro.metagenomics.jobManager.GenericJobCommand;
import uk.ac.ebi.interpro.metagenomics.jobManager.SystemCommand;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Submits a batch job to LSF.
 *
 * @author Maxim Scheremetjew, EMBL-EBI, InterPro
 * @version $Id: LSFBsubCommand.java,v 1.6 2013/11/28 17:00:40 matthew Exp $
 * @since 1.0-SNAPSHOT
 */
public class LSFBsubCommand extends GenericJobCommand<LSFBsubOption> {
    protected final SystemCommand systemCmd;

    public LSFBsubCommand(SystemCommand systemCmd) {
        this(systemCmd, new HashMap<LSFBsubOption, Object>());
    }

    public LSFBsubCommand(SystemCommand systemCmd, Map<LSFBsubOption, Object> options) {
        super("bsub", options);
        this.systemCmd = systemCmd;
    }

    public SystemCommand getSystemCmd() {
        return systemCmd;
    }

    @Override
    public String toString() {
        final String WHITESPACE = " ";
        StringBuilder result = new StringBuilder();
        for (String cmd : getCommand()) {
            result.append(cmd + WHITESPACE);
        }
        return result.toString().trim();
    }

    @Override
    public List<String> getCommand() {
        List<String> result = new ArrayList<String>();
        result.add(getJobSchedulerCommand());
        for (LSFBsubOption optionKey : getOptions().keySet()) {
            String lsfShortOption = optionKey.getShortOpt();
            String argument = String.valueOf(getOptionValue(optionKey));
            result.add(lsfShortOption);
            result.add(argument);
        }
        for (String cmd : getSystemCmd().getCommand()) {
            result.add(cmd);
        }
        return result;
    }
}
