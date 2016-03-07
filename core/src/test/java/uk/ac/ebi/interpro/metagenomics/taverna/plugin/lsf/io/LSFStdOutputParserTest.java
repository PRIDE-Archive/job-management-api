package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.io;

import junit.framework.Assert;
import org.junit.Test;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.BaseTestClass;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.statistics.LSFJobStatistics;

/**
 * Created with IntelliJ IDEA.
 * User: craigm
 * Date: 24/08/12
 * Time: 10:30
 * To change this template use File | Settings | File Templates.
 */
public class LSFStdOutputParserTest extends BaseTestClass {


    @Test
    /**
     * Tests that the parser recognises the LSF output file is incomplete
     * ie the job was not successful
     */
    public void checkLSFIncompleteOutputFile() {
        String lsfStdOutputFilePath = getResourceFilePath("incomplete-lsf.out");
        LSFStdOutputParser parser = new LSFStdOutputParser();
        LSFJobStatistics stats = parser.getJobStdOutputStatistics(lsfStdOutputFilePath);
        Assert.assertNotNull(stats);
        Assert.assertFalse("Did not detect the job failed from incomplete LSF output file" + lsfStdOutputFilePath,
                stats.isOK());
    }


    @Test
    /**
     * Test that the parser can successfully check an LSF output file
     * with a single message
     */
    public void checkLSFSingleMessageOutputFile() {
        String lsfStdOutputFilePath = getResourceFilePath("failed.out.bak");
        LSFStdOutputParser parser = new LSFStdOutputParser();
        LSFJobStatistics stats = parser.getJobStdOutputStatistics(lsfStdOutputFilePath);
        Assert.assertNotNull(stats);
        Assert.assertFalse("Did not detect failed job from LSF output file" + lsfStdOutputFilePath,
                           stats.isOK());

    }

    @Test
    /**
     *  Test that the parser can successfully check an LSF output file
     *  with multiple messages
     */
    public void checkLSFMultiMessageOutputFile() {
        String lsfStdOutputFilePath = getResourceFilePath("multimessage-lsf.out");
        LSFStdOutputParser parser = new LSFStdOutputParser();
        LSFJobStatistics stats = parser.getJobStdOutputStatistics(lsfStdOutputFilePath);
        Assert.assertNotNull(stats);
        Assert.assertTrue("Did not detect successful job at end of multi-message LSF output file" + lsfStdOutputFilePath,
                          stats.isOK());
    }

}
