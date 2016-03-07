package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model;

import uk.ac.ebi.interpro.metagenomics.jobManager.GenericJob;

/**
 * Represents a LSF job.
 *
 * @author Maxim Scheremetjew, EMBL-EBI, InterPro
 * @version $Id: LSFJob.java,v 1.4 2013/11/25 09:42:26 craigm Exp $
 * @since 1.0-SNAPSHOT
 */
public class LSFJob extends GenericJob<LSFBsubCommand>  {


    public LSFJob(LSFBsubCommand jobSchedulerCommand) {
        super(jobSchedulerCommand);
    }


}
