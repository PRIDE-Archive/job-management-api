package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model;

import uk.ac.ebi.interpro.metagenomics.jobManager.GenericJobCommand;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Displays information about LSF jobs.
 *
 * @author Maxim Scheremetjew, EMBL-EBI, InterPro
 * @version $Id: LSFBjobsCommand.java,v 1.5 2013/11/26 15:35:43 matthew Exp $
 * @since 1.0-SNAPSHOT
 */
public class LSFBjobsCommand extends GenericJobCommand<LSFBjobsOption> {


    public LSFBjobsCommand() {
        super("bjobs");
    }

    public LSFBjobsCommand(Map<LSFBjobsOption, Object> options) {
        super("bjobs", options );
    }

    @Override
    public List<String> getCommand() {
        List<String> result = new ArrayList<String>();
        result.add(getJobSchedulerCommand());
        for (LSFBjobsOption optionKey : getOptions().keySet()) {
            String lsfShortOption = optionKey.getShortOpt();
            result.add(lsfShortOption);
            if (optionKey.isArgumentRequired()) {
                result.add(String.valueOf(getOptionValue(optionKey)));
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return null;
    }
}
