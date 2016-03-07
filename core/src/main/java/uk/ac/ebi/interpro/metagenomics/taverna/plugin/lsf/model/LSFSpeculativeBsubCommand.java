package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model;

import uk.ac.ebi.interpro.metagenomics.jobManager.SpeculativeCommand;

import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: craigm
 * Date: 16/10/13
 * Time: 11:14
 * To change this template use File | Settings | File Templates.
 */
public class LSFSpeculativeBsubCommand extends LSFBsubCommand {

    public LSFSpeculativeBsubCommand(SpeculativeCommand command, Map<LSFBsubOption, Object> options) {
        super(command, options);
    }

}
