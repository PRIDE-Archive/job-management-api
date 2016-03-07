package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model;

import junit.framework.Assert;
import org.junit.Test;
import uk.ac.ebi.interpro.metagenomics.jobManager.SystemCommand;

/**
 * Represents a JUnit test for {@link LSFBsubCommand}.
 *
 * @author Maxim Scheremetjew, EMBL-EBI, InterPro
 * @version $Id: LSFBsubCommandTest.java,v 1.3 2013/11/28 17:00:40 matthew Exp $
 * @since 1.0-SNAPSHOT
 */
public class LSFBsubCommandTest {

    @Test
    public void testCreateBsubCommandInstance() {
        //Build system command
        SystemCommand systemCommand = new SystemCommand("ls", "-l");
        //Create bsub command instance
        LSFBsubCommand lsfBsubCommand = new LSFBsubCommand(systemCommand);
        String queueName = "production-rh6";
        lsfBsubCommand.putCommandOption(LSFBsubOption.QUEUE_NAME, queueName);
        lsfBsubCommand.putCommandOption(LSFBsubOption.JOB_NAME, "jobName");
        lsfBsubCommand.putCommandOption(LSFBsubOption.STD_OUTPUT_FILE, "test.out");
        lsfBsubCommand.putCommandOption(LSFBsubOption.STD_ERROR_OUTPUT_FILE, "test.err");
        lsfBsubCommand.putCommandOption(LSFBsubOption.MIN_MAX_PROCESSORS, "2,8");
        final int maxMem = 500;
        lsfBsubCommand.putCommandOption(LSFBsubOption.MAX_MEMORY, maxMem);
        lsfBsubCommand.putCommandOption(LSFBsubOption.RESOURCE_REQUIREMENT, "\"rusage[mem=" + maxMem + "]\"");
        //Run JUnit tests
        Assert.assertNotNull("Unexpected bsub command!", lsfBsubCommand.getCommand());
        Assert.assertEquals("Unexpected bsub command size!", 17, lsfBsubCommand.getCommand().size());
        Assert.assertNotNull("Unexpected bsub command!", lsfBsubCommand.getJobSchedulerCommand());
        Assert.assertEquals("Unexpected bsub command!", "bsub", lsfBsubCommand.getJobSchedulerCommand());
        Assert.assertNotNull("Unexpected bsub command options!", lsfBsubCommand.getOptions());
        Assert.assertEquals("Unexpected bsub command option size!", 7, lsfBsubCommand.getOptions().size());
        Assert.assertEquals("Unexpected bsub command option value!", queueName, lsfBsubCommand.getOptionValue(LSFBsubOption.QUEUE_NAME));
        Assert.assertEquals("Unexpected bsub command option value!", maxMem, lsfBsubCommand.getOptionValue(LSFBsubOption.MAX_MEMORY));
        Assert.assertEquals("Unexpected bsub command option value!", "\"rusage[mem=" + maxMem + "]\"", lsfBsubCommand.getOptionValue(LSFBsubOption.RESOURCE_REQUIREMENT));
        //
        Assert.assertNotNull("Unexpected system command!", lsfBsubCommand.getSystemCmd());
        Assert.assertNotNull("Unexpected system command list!", lsfBsubCommand.getSystemCmd().getCommand());
        Assert.assertEquals("Unexpected system command size!", 2, lsfBsubCommand.getSystemCmd().getCommand().size());
        Assert.assertEquals("Unexpected system command list entry!", "ls", lsfBsubCommand.getSystemCmd().getCommand().get(0));
        Assert.assertEquals("Unexpected system command list entry!", "-l", lsfBsubCommand.getSystemCmd().getCommand().get(1));
        //
        String expected = "bsub -q production-rh6 -J jobName -o test.out -e test.err -n 2,8 -M 500 -R " + "\"rusage[mem=500]\"" + " ls -l";
        String actual = lsfBsubCommand.toString();
        Assert.assertEquals(expected.length(), actual.length());
        Assert.assertTrue(actual.contains("bsub"));
        Assert.assertTrue(actual.contains("-q production-rh6"));
        Assert.assertTrue(actual.contains("-J jobName"));
        Assert.assertTrue(actual.contains("-o test.out"));
        Assert.assertTrue(actual.contains("-e test.err"));
        Assert.assertTrue(actual.contains("-n 2,8"));
        Assert.assertTrue(actual.contains("-M 500"));
        Assert.assertTrue(actual.contains("ls -l"));
        Assert.assertTrue(actual.contains("-R " + "\"rusage[mem=500]\""));
    }
}
