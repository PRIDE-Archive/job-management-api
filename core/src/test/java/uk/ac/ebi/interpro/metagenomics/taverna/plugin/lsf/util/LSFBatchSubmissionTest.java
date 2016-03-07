package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.util;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import uk.ac.ebi.interpro.metagenomics.jobManager.SystemCommand;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.management.LSFManager;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.LSFBatchItem;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.LSFBsubOption;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.LSFJobBatch;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.LSFResubmissionStrategy;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.statistics.LSFJobBatchStatisticsSummary;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;

/**
 * Represents a real coding test for submitting jobs on the LSF farm.
 *
 * @author Maxim Scheremetjew, EMBL-EBI, InterPro
 * @version $Id: LSFBatchSubmissionTest.java,v 1.7 2015/04/29 10:25:23 matthew Exp $
 * @since 1.0-SNAPSHOT
 */
public class LSFBatchSubmissionTest {

    @Test
    @Ignore
    public void testSubmitBatch() throws FileNotFoundException {
        Set<LSFBatchItem> batchItems = new HashSet();
        //Create 100 batch items and add them to the collection
        // TODO Don't use a file in Craig's home directory! The chunkedSequence.list is no longer available in that location anyway
        Scanner scanner = new Scanner(new BufferedReader(new FileReader(new File("/homes/craigm/python-projects/Metagenomics/testPara/chunkedSequence.list"))));
        while (scanner.hasNextLine()) {
            String inputFile = scanner.nextLine().trim();
            //Create system commands
            SystemCommand systemCmd =
                    new SystemCommand("/nfs/seqdb/production/interpro/development/metagenomics/pipeline/tools/RepeatMasker-open-3.2.2/RepeatMasker", inputFile);
            //Setting LSF bsub options
            Map<LSFBsubOption, Object> options = new HashMap<LSFBsubOption, Object>(2);
            options.put(LSFBsubOption.QUEUE_NAME, "production-rh6");
            options.put(LSFBsubOption.STD_OUTPUT_FILE, inputFile + ".log");
            batchItems.add(new LSFBatchItem(systemCmd, options));
        }
        //Create LSF batch
        LSFJobBatchFactory batchFactory = LSFManager.getLSFJobBatchFactory();
        LSFJobBatch jobBatch = batchFactory.createLSFBatch(batchItems);
        //Submit LSF batch
        LSFJobSubmitter jobSubmitter = LSFManager.getLSFJobSubmitter();
        jobSubmitter.submitBatch(jobBatch);
        //Monitor LSF batch
        LSFBatchMonitor monitor = LSFManager.getLSFBatchMonitor();
        LSFJobBatchStatisticsSummary summary = monitor.monitorBatch(jobBatch, new LSFResubmissionStrategy(), 60000);
        Assert.assertNotNull(summary);
        System.out.println("Monitor result " + summary.isAllSuccessful());
    }
}
