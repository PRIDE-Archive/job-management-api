package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model;

import uk.ac.ebi.interpro.metagenomics.jobManager.GenericJobCommand;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: craigm
 * Date: 30/10/13
 * Time: 10:48
 * To change this template use File | Settings | File Templates.
 */
public class LSFBkillCommand extends GenericJobCommand<LSFBkillOption> {

    public LSFBkillCommand() {
        super("bkill");
    }

    public LSFBkillCommand(Map<LSFBkillOption, Object> options) {
        super("bkill", options );
    }

    @Override
    public List<String> getCommand() {
        List<String> result = new ArrayList<String>();
        result.add(getJobSchedulerCommand());
        for (LSFBkillOption optionKey : getOptions().keySet()) {
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
        List<String> command = getCommand();
        return Arrays.toString(command.toArray());
    }

}
