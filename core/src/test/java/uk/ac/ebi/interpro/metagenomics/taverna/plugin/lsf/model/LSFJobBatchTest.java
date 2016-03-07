package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model;

import junit.framework.Assert;
import org.junit.Test;
import uk.ac.ebi.interpro.metagenomics.jobManager.SystemCommand;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.management.LSFManager;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.util.LSFJobBatchFactory;

import java.io.File;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: craigm
 * Date: 17/09/12
 * Time: 16:37
 * To change this template use File | Settings | File Templates.
 */
public class LSFJobBatchTest {

    @Test
    public void testSerialization() {
        Set<LSFBatchItem> batchItems = new HashSet<LSFBatchItem>();
        SystemCommand systemCommand = new SystemCommand("ls", "-l");
        List<String> outputFiles = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            Map<LSFBsubOption, Object> options = new HashMap<LSFBsubOption, Object>();
            String fileName = String.valueOf(i) + ".out";
            outputFiles.add(fileName);
            options.put(LSFBsubOption.STD_OUTPUT_FILE, fileName);
            batchItems.add(new LSFBatchItem(systemCommand, options));

        }
        LSFJobBatchFactory lsfJobBatchFactory = LSFManager.getLSFJobBatchFactory();
        LSFJobBatch jobBatch = lsfJobBatchFactory.createLSFBatch(batchItems);
        jobBatch.serialize("serialized.batch");
        LSFJobBatch serializedJobBatch = LSFJobBatch.unSerialize("serialized.batch");
        removeFile("serialized.batch");
        Assert.assertEquals("Serialized batch size not equal to the original batch size", serializedJobBatch.getBatchSize(), jobBatch.getBatchSize());
        Assert.assertEquals("Serialized batch project name is different to the original batch name", serializedJobBatch.getProjectName(), jobBatch.getProjectName());
        Assert.assertEquals("Serialized job batch is not equal to original job batch ", jobBatch, serializedJobBatch);
    }

    @Test
    public void testJobBatchCreation() {
        List<List<String>> chunkFile2CommandMap = new ArrayList<List<String>>();
        List<String> item1 = new ArrayList<String>();
        item1.add("/home/user/input.file");
        item1.add("ls -lh /home/user/input.file");
        chunkFile2CommandMap.add(item1);
        List<String> item2 = new ArrayList<String>();
        item2.add("/home/user/input2.file");
        item2.add("ls -lh /home/user/input2.file");
        chunkFile2CommandMap.add(item2);
        //
        String lsfLogFileDirectory = "/home/user/temp/";

        //Create collection of batch jobs
        Set<LSFBatchItem> batchItems = new HashSet<LSFBatchItem>();

        for (List<String> chunk2Command : chunkFile2CommandMap) {
            File chunkedFile = new File(chunk2Command.get(0));
            List command = new ArrayList(1);
            command.add(chunk2Command.get(1));
            SystemCommand systemCmd = new SystemCommand(command);

            //Setting LSF bsub options
            Map options = new HashMap();
            options.put(LSFBsubOption.QUEUE_NAME, "production");
            options.put(LSFBsubOption.MIN_MAX_PROCESSORS, "2,8");
            options.put(LSFBsubOption.MAX_MEMORY, "2000");
            options.put(LSFBsubOption.STD_OUTPUT_FILE, lsfLogFileDirectory + chunkedFile.getName() + ".lsf.out");
            options.put(LSFBsubOption.STD_ERROR_OUTPUT_FILE, lsfLogFileDirectory + chunkedFile.getName() + ".lsf.err");
            batchItems.add(new LSFBatchItem(systemCmd, options));
        }
        LSFJobBatchFactory batchFactory = LSFManager.getLSFJobBatchFactory();
        LSFJobBatch batch = batchFactory.createLSFBatch(batchItems);
        Assert.assertEquals(2, batch.getBatchSize());
        Collection<LSFJob> lsfJobs = batch.getJobs();
        for (LSFJob job : lsfJobs) {
            LSFBsubCommand bsubCommand = job.getJobSchedulerCommand();
            //Check if it contains either this or that
            Assert.assertTrue(bsubCommand.toString().contains("ls -lh /home/user/input.file") || bsubCommand.toString().contains("ls -lh /home/user/input2.file"));

            if (bsubCommand.toString().contains("ls -lh /home/user/input.file")) {
                //Check certain options
                Assert.assertTrue(bsubCommand.toString().contains("-M 2000 "));
                Assert.assertTrue(bsubCommand.toString().contains("-q production "));
                Assert.assertTrue(bsubCommand.toString().contains("-n 2,8 "));
                Assert.assertTrue(bsubCommand.toString().contains("-o /home/user/temp/input.file.lsf.out"));
                Assert.assertTrue(bsubCommand.toString().contains("-e /home/user/temp/input.file.lsf.err"));
                Assert.assertTrue(bsubCommand.toString().contains("-P proj"));
                Assert.assertTrue(bsubCommand.toString().contains("-J proj"));
            } else if (bsubCommand.toString().contains("ls -lh /home/user/input2.file")) {
                //Check certain options
                Assert.assertTrue(bsubCommand.toString().contains("-M 2000 "));
                Assert.assertTrue(bsubCommand.toString().contains("-q production "));
                Assert.assertTrue(bsubCommand.toString().contains("-n 2,8 "));
                Assert.assertTrue(bsubCommand.toString().contains("-o /home/user/temp/input2.file.lsf.out"));
                Assert.assertTrue(bsubCommand.toString().contains("-e /home/user/temp/input2.file.lsf.err"));
                Assert.assertTrue(bsubCommand.toString().contains("-P proj"));
                Assert.assertTrue(bsubCommand.toString().contains("-J proj"));
            }
        }
//        batch.serialize(serializationFile);
    }

    @Test
    public void testForDuplicateBatchItems() {

        final SystemCommand command1 = new SystemCommand("ls", "-l");
        final SystemCommand command2 = new SystemCommand("ls", "-las");

        final Map<LSFBsubOption, Object> map1 = new HashMap<LSFBsubOption, Object>();
        map1.put(LSFBsubOption.STD_OUTPUT_FILE, "output1.out");

        final Map<LSFBsubOption, Object> map2 = new HashMap<LSFBsubOption, Object>();
        map2.put(LSFBsubOption.STD_OUTPUT_FILE, "output2.out");
        map2.put(LSFBsubOption.QUEUE_NAME, "queueName");

        final Set<LSFBatchItem> batchItems = new HashSet<LSFBatchItem>();

        // Add first batch item to set
        batchItems.add(new LSFBatchItem(command1, map1));
        Assert.assertEquals(1, batchItems.size());

        // Same command but different options, added
        batchItems.add(new LSFBatchItem(command1, map2));
        Assert.assertEquals(2, batchItems.size());

        // Different command, added
        batchItems.add(new LSFBatchItem(command2, map2));
        Assert.assertEquals(3, batchItems.size());

        // Same command and options, not added
        batchItems.add(new LSFBatchItem(command2, map2));
        Assert.assertEquals(3, batchItems.size());

        boolean threwEx = false;
        try {
            // Options cannot be NULL
            batchItems.add(new LSFBatchItem(command1, null));
        }
        catch (IllegalArgumentException ex) {
            threwEx = true;
        }
        Assert.assertTrue(threwEx);
    }

    private void removeFile(String fileName) {

        File file = new File(fileName);
        if (file.exists()) {
            file.delete();
        }

    }

}
