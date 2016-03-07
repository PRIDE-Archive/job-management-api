package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.management;

import uk.ac.ebi.interpro.metagenomics.jobManager.SystemCommand;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.io.LSFJobInfoParser;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.util.*;

/**
 * Represents the entry point for using the LSF API.
 * <p/>
 * Provides access to all API builders (factories).
 *
 * @author Maxim Scheremetjew, EMBL-EBI, InterPro
 * @version $Id: LSFManager.java,v 1.7 2013/11/25 09:42:26 craigm Exp $
 * @since 1.0-SNAPSHOT
 */
public class LSFManager {

    public static LSFJobBatchFactory getLSFJobBatchFactory() {
        return LSFJobBatchFactory.getInstance();
    }

    public static LSFJobSubmitter getLSFJobSubmitter() {
        return LSFJobSubmitter.getInstance();
    }

    public static LSFBatchMonitor getLSFBatchMonitor() {
        return new LSFBatchMonitor();
    }

    public static LSFJobInfoParser getLSFJobInfoParser() {
        return LSFJobInfoParser.getInstance();
    }

    public static LSFBsubCommandBuilder getLSFBsubCommandBuilder(SystemCommand cmd) {
        return new LSFBsubCommandBuilder(cmd);
    }

    public static LSFJobKiller getLSFJobKiller() {
        return LSFJobKiller.getInstance();
    }
}