package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.statistics;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.BaseTestClass;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.management.LSFManager;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.LSFJobBatch;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.LSFResubmissionStrategy;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.util.LSFBatchMonitor;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.util.LSFJobSubmitter;

/**
 * Check a list of LSF standard output text can be parsed and the statistics summarised.
 *
 * @author Matthew Fraser, EMBL-EBI, InterPro
 * @version $Id: LSCommandsBatchTest.java,v 1.2 2013/11/26 15:35:43 matthew Exp $
 * @since 1.0-SNAPSHOT
 */
public class LSCommandsBatchTest extends BaseTestClass {

    @Test
    @Ignore
    public void summariseBatchStatistics() {

        if (!isLSFAvailable()) {
            return;
        }

        // The stepName-batchSerialisation.tmp file is a serialised batch of jobs, containing LSF output from
        // the 2 batch2/ls_1.out and ls_2.out jobs
        LSFJobBatch jobBatch = LSFJobBatch.unSerialize(getResourceFilePath("batch2/stepName-batchSerialisation.tmp"));
        LSFJobSubmitter jobSubmitter = LSFManager.getLSFJobSubmitter();
       	jobSubmitter.submitBatch(jobBatch);
        LSFBatchMonitor monitor = LSFManager.getLSFBatchMonitor();
        LSFJobBatchStatisticsSummary summary=monitor.monitorBatch(jobBatch, new LSFResubmissionStrategy(), 10000);
        Assert.assertNotNull(summary);

    }
}
