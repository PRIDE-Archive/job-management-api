package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.util;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import uk.ac.ebi.interpro.metagenomics.jobManager.SingleOutputFileCommand;
import uk.ac.ebi.interpro.metagenomics.jobManager.SpeculativeCommand;
import uk.ac.ebi.interpro.metagenomics.jobManager.SystemCommand;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.BaseTestClass;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.management.LSFManager;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.*;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.statistics.LSFJobBatchStatisticsSummary;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * Represents a JUnit test for class {@link LSFBatchMonitor}.
 *
 * @author Maxim Scheremetjew, EMBL-EBI, InterPro
 * @version $Id: LSFBatchMonitorTest.java,v 1.23 2015/04/29 10:25:23 matthew Exp $
 * @since 1.0-SNAPSHOT
 */
public class LSFBatchMonitorTest extends BaseTestClass {
    private static final Logger LOGGER = Logger.getLogger(LSFBatchMonitorTest.class.getName());


    @Test
    public void testBatchSerializability() {
        Set<LSFBatchItem> batchItems = new HashSet<LSFBatchItem>();
        for (int i=0; i < 10; i++) {
            SystemCommand systemCommand = new SystemCommand("echo " + i);
            Map<LSFBsubOption, Object> options = new HashMap<LSFBsubOption, Object>();
            options.put(LSFBsubOption.STD_OUTPUT_FILE, "test" + i );
            LSFBatchItem batchItem = new LSFBatchItem(systemCommand, options);
            batchItems.add(batchItem);
        }
        LSFJobBatch batch = LSFJobBatchFactory.getInstance().createLSFBatch(batchItems);
        batch.serialize("test");
        LSFJobBatch newbatch = LSFJobBatch.unSerialize("test");
        Assert.assertEquals(batch, newbatch);


    }

    @Test
    public void testSpeculativeBatchSerializability() {
        Set<LSFSpeculativeBatchItem> batchItems = new HashSet<LSFSpeculativeBatchItem>();
        for (int i=0; i < 10; i++) {
            SpeculativeCommand systemCommand = new SingleOutputFileCommand(">", "echo", "1", ">", Integer.toString(i));
            Map<LSFBsubOption, Object> options = new HashMap<LSFBsubOption, Object>();
            options.put(LSFBsubOption.STD_OUTPUT_FILE, "test" + i );
            LSFSpeculativeBatchItem batchItem = new LSFSpeculativeBatchItem(systemCommand, options);
            batchItems.add(batchItem);
        }
        LSFSpeculativeJobBatch batch = LSFJobBatchFactory.getInstance().createSpeculativeLSFBatch(batchItems);
        batch.serialize("test-spec");
        LSFSpeculativeJobBatch newBatch = LSFSpeculativeJobBatch.unSerialize("test-spec");
        Assert.assertEquals(batch, newBatch);

    }

    @Test
    public void testBuildBjobsCommand() {

        String projectName = "testProject";
        LSFBatchMonitor monitor = LSFManager.getLSFBatchMonitor();
        LSFBjobsCommand cmd = monitor.buildBjobsCommand(projectName);
        List<String> cmdList = cmd.getCommand();
        Assert.assertTrue("Bjobs command doesn't contain the term 'bjobs'! Command list: " + cmdList, cmdList.contains("bjobs"));
        Assert.assertTrue("Batched bjobs command doesn't contain the project name option. Command list: " + cmdList,
                cmdList.contains(LSFBjobsOption.PROJECT_NAME.getShortOpt()));
        Assert.assertTrue("Batched bjobs command doesn't contain the project name option. Command list: " + cmdList,
                cmdList.contains(projectName));
        Assert.assertTrue("Bjobs command doesn't specify wide format (-w). Command list: " + cmdList,
                cmdList.contains(LSFBjobsOption.WIDE_FORMAT.getShortOpt()));
        Assert.assertTrue("Bjobs command doesn't request jobs in all states (-a). Command list: " + cmdList,
                cmdList.contains(LSFBjobsOption.SHOW_ALL_STATES.getShortOpt()));

        Assert.assertEquals("Bjobs command not of expected size. Command list: " + cmdList, cmdList.size(), 5);
    }

    @Test
    @Ignore
    public void testMonitorBatch() throws Exception {
        if (!isLSFAvailable()) {
            return;
        }

        System.out.println("LSF available");
        Set<LSFBatchItem> batchItems = new HashSet<LSFBatchItem>();
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
        Assert.assertTrue("LSF job batch did not complete successfully", summary.isAllSuccessful());
    }

    @Test
    /**
     * This test runs and monitors a batch of 10 very quick lsf jobs ("ls -l")
     */
    public void testMonitorBatchWithoutResubmission() {
        if (!isLSFAvailable()) {
            return;
        }
        Set<LSFBatchItem> batchItems = new HashSet<LSFBatchItem>();
        SystemCommand systemCommand = new SystemCommand("ls", "-l");
        List<String> outputFiles = new ArrayList<String>();
        for (int i = 0; i < 10; i ++) {
            Map<LSFBsubOption, Object> options = new HashMap<LSFBsubOption, Object>();
            String fileName = String.valueOf(i) +".out";
            outputFiles.add(fileName);
            options.put(LSFBsubOption.STD_OUTPUT_FILE, fileName);
            batchItems.add(new LSFBatchItem(systemCommand, options));

        }
        LSFJobBatchFactory factory = LSFJobBatchFactory.getInstance();
        LSFJobBatch batch = factory.createLSFBatch(batchItems);
        LSFJobSubmitter jobSubmitter = LSFManager.getLSFJobSubmitter();
        LSFBatchMonitor monitor = LSFManager.getLSFBatchMonitor();
        jobSubmitter.submitBatch(batch);
        LSFJobBatchStatisticsSummary summary = monitor.monitorBatch(batch, new LSFResubmissionStrategy(), 10000);
        Assert.assertNotNull(summary);
        Assert.assertTrue("LSF job batch did not complete successfully", summary.isAllSuccessful());
        // cleanup the output files
        for (String fileName: outputFiles) {
            File file = new File(fileName);
            file.delete();
        }
    }

    @Test
    /**
     * This test creates a job batch containing a single job and generates an lsf
     * output file that indicates the job failed.
     * The batch is not submitted to lsf but is passed directly to the monitor
     * which should check the lsf output file, detect the failure and resubmit the job (successfully!)
     */
    public void testResubmission() {
        if (!isLSFAvailable()) {
            return;
        }

        LOGGER.info("LSF is available - testing job resubmission");
        // copy the failed lsf output from the backup copy to the working file
        String backupFile = getResourceFilePath("failed.out.bak");
        String lsfOutputFile = getResourceFilePath("failed.out");
        copyFile(backupFile, lsfOutputFile);

        SystemCommand systemCommand = new SystemCommand("ls", "-l");
        Map<LSFBsubOption, Object> options = new HashMap<LSFBsubOption, Object>();
        options.put(LSFBsubOption.QUEUE_NAME, "production-rh6");
        options.put(LSFBsubOption.STD_OUTPUT_FILE, lsfOutputFile);
        Set<LSFBatchItem> batch = new HashSet<LSFBatchItem>();
        batch.add(new LSFBatchItem(systemCommand, options));
        LSFJobBatch lsfBatch = LSFJobBatchFactory.getInstance().createLSFBatch(batch);
        LSFBatchMonitor monitor = LSFManager.getLSFBatchMonitor();
        LSFResubmissionStrategy resubmissionStrategy= new LSFResubmissionStrategy(2);
        LSFJobBatchStatisticsSummary summary = monitor.monitorBatch(lsfBatch, resubmissionStrategy, 10000);
        Assert.assertNotNull(summary);
        Assert.assertTrue("LSF job batch resubmission was not successful", summary.isAllSuccessful());

    }



    @Test
    /**
     * As above, this test creates a job batch containing a single job and generates an lsf
     * output file that indicates the job failed.
     * The batch is not submitted to lsf but is passed directly to the monitor
     * which should check the lsf output file, and detect the failure
     * In this case however, as we have set the maximum allowed failures to 1 then
     * the job cannot be resubmitted and the batch fails
     */
    public void testResubmissionLimit() {
        if (!isLSFAvailable()) {
            return;
        }

        LOGGER.info("LSF is available - testing job resubmission");
        // copy the failed lsf output from the backup copy to the working file
        String backupFile = getResourceFilePath("failed.out.bak");
        String lsfOutputFile = getResourceFilePath("failed.out");
        copyFile(backupFile, lsfOutputFile);

        SystemCommand systemCommand = new SystemCommand("ls", "-l");
        Map<LSFBsubOption, Object> options = new HashMap<LSFBsubOption, Object>();
        options.put(LSFBsubOption.QUEUE_NAME, "production-rh6");
        options.put(LSFBsubOption.STD_OUTPUT_FILE, lsfOutputFile);
        Set<LSFBatchItem> batch = new HashSet<LSFBatchItem>();
        batch.add(new LSFBatchItem(systemCommand, options));
        LSFJobBatch lsfBatch = LSFJobBatchFactory.getInstance().createLSFBatch(batch);
        LSFBatchMonitor monitor = LSFManager.getLSFBatchMonitor();
        LSFResubmissionStrategy resubmissionStrategy= new LSFResubmissionStrategy(1);
        LSFJobBatchStatisticsSummary summary = monitor.monitorBatch(lsfBatch, resubmissionStrategy, 10000);
        Assert.assertNotNull(summary);
        Assert.assertFalse("LSF job batch resubmission did not detect that the maximum allowed failures were reached"
                , summary.isAllSuccessful());

    }



    public void copyFile(String  sourceFileName, String destFileName)   {
        try {

            File sourceFile = new File(sourceFileName);
            File destFile = new File(destFileName);
            if(!destFile.exists()) {
                destFile.createNewFile();
            }
            FileChannel source = null;
            FileChannel destination = null;
            source = new FileInputStream(sourceFile).getChannel();
            destination = new FileOutputStream(destFile).getChannel();
            destination.transferFrom(source, 0, source.size());

            if(source != null) {
                source.close();
            }
            if(destination != null) {
                destination.close();
            }
        } catch (IOException e) {
            throw new IllegalStateException("Problem copying file " + sourceFileName + " to " + destFileName, e);
        }

    }


}
