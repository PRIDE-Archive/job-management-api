package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.util;

import junit.framework.Assert;
import org.junit.Test;
import uk.ac.ebi.interpro.metagenomics.jobManager.SystemCommand;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.LSFBsubCommand;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.LSFBsubOption;

/**
 * Represents a JUnit test for the class {@link LSFBsubCommandBuilder}.
 *
 * @author Maxim Scheremetjew, EMBL-EBI, InterPro
 * @version $Id: LSFBsubCommandBuilderTest.java,v 1.8 2013/11/28 17:00:40 matthew Exp $
 * @since 1.0-SNAPSHOT
 */
public class LSFBsubCommandBuilderTest {

    @Test
    public void testLSFBsubCommandBuilder() {
        //Build system command
        SystemCommand systemCommand = new SystemCommand("ls", "-l");
        //Create bsub command instance using command builder
        String queueName = "production-rh6";
        String jobName = "jobName";
        LSFBsubCommandBuilder cmdBuilder = new LSFBsubCommandBuilder(systemCommand);
        cmdBuilder.setQueueName(queueName);
        cmdBuilder.setJobName(jobName);
        cmdBuilder.setStdOutputFile("test.out");
        cmdBuilder.setStdErrOutputFile("test.err");
        cmdBuilder.setMinProcessors(2);
        cmdBuilder.setMaxProcessors(8);
        final int maxMem = 500;
        cmdBuilder.setMaxMemory(maxMem);
        cmdBuilder.setResourceRequirement("\"rusage[mem=" + maxMem + "]\"");
        LSFBsubCommand lsfBsubCommand = cmdBuilder.build();
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
