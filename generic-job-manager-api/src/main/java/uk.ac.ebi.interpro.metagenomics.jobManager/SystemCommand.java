package uk.ac.ebi.interpro.metagenomics.jobManager;

import java.util.Arrays;
import java.util.List;

/**
 * Represents a system command which you would like to submit on a cluster.
 *
 * @author Maxim Scheremetjew, EMBL-EBI, InterPro
 * @version $Id: SystemCommand.java,v 1.4 2013/11/25 09:42:26 craigm Exp $
 * @since 1.0-SNAPSHOT
 */
public class SystemCommand extends GenericCommand {

    public SystemCommand(List<String> command) {
        super(command);
    }

    public SystemCommand(String... command) {
        super(Arrays.asList(command));
    }



}