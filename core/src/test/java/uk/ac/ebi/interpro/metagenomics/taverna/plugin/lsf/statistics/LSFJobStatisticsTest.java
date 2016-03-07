package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.statistics;

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.BaseTestClass;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.io.LSFStdOutputParser;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.io.ParseException;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Check LSF standard output for a job can be parsed and a {@link LSFJobStatistics} object populated.
 *
 * @author Matthew Fraser, EMBL-EBI, InterPro
 * @version $Id: LSFJobStatisticsTest.java,v 1.3 2013/11/25 09:42:26 craigm Exp $
 * @since 1.0-SNAPSHOT
 */
public class LSFJobStatisticsTest extends BaseTestClass {

    private static final Logger LOGGER = Logger.getLogger(LSFJobStatisticsTest.class.getName());

    @Test
    /**
     * Tests that the parser recognises the LSF output file is complete.
     */
    public void checkLSFCompleteOutputFile() {
        final String lsfStdOutputFilePath = getResourceFilePath("batch/bsub_factorial_1.out");
        final LSFStdOutputParser parser = new LSFStdOutputParser();
        final LSFJobStatistics stats = parser.getJobStdOutputStatistics(lsfStdOutputFilePath);

        final Date expectedStartDateTime;
        final Date expectedEndDateTime;
        try {
            expectedStartDateTime = new SimpleDateFormat("EEE MMM d hh:mm:ss yyyy").parse("Thu Oct 17 10:37:28 2013");
            expectedEndDateTime = new SimpleDateFormat("EEE MMM d hh:mm:ss yyyy").parse("Thu Oct 17 10:42:53 2013");
        } catch (java.text.ParseException e) {
            Assert.fail("Test in error");
            return;
        }

        Assert.assertNotNull(stats);
        Assert.assertEquals(311845, stats.getJobId());
        Assert.assertEquals(expectedStartDateTime, stats.getStartDateTime());
        Assert.assertEquals(expectedEndDateTime, stats.getEndDateTime());
        Assert.assertEquals(new Double(114.75), stats.getCpuTime());
        Assert.assertEquals(new Long(11), stats.getMaxMemory());
        Assert.assertEquals(new Long(237), stats.getMaxSwap());
        Assert.assertEquals(new Integer(3), stats.getMaxProcesses());
        Assert.assertEquals(new Integer(4), stats.getMaxThreads());
        Assert.assertEquals(true, stats.isOutputComplete());
        Assert.assertEquals(true, stats.isJobSuccessful());
        Assert.assertEquals(true, stats.isOK());
    }

    @Ignore //(expected = ParseException.class)
    /**
     * Tests that the parser recognises the LSF output file is unsuccessful and incomplete.
     */
    public void checkLSFUnsuccessfulOutputFile() {
        final String lsfStdOutputFilePath = getResourceFilePath("batch/bsub_factorial_4.out");
        final LSFStdOutputParser parser = new LSFStdOutputParser();
        parser.getJobStdOutputStatistics(lsfStdOutputFilePath);
    }

    @Test
    /**
     * Tests that the parser recognises the LSF output file was successful but incomplete.
     */
    public void checkLSFIncompleteOutputFile() {
        final String lsfStdOutputFilePath = getResourceFilePath("batch/bsub_factorial_5.out");
        final LSFStdOutputParser parser = new LSFStdOutputParser();
        final LSFJobStatistics stats = parser.getJobStdOutputStatistics(lsfStdOutputFilePath);
        Assert.assertNotNull(stats);
        Assert.assertEquals(false, stats.isOutputComplete());
        Assert.assertEquals(true, stats.isJobSuccessful());
        Assert.assertEquals(false, stats.isOK());
    }

    @Test
    /**
     * Tests that the parser recognises the LSF output file was sucessful but incomplete.
     */
    public void checkLSFCompleteNoOptionalFieldsOutputFile() {
        final String lsfStdOutputFilePath = getResourceFilePath("batch/bsub_factorial_6.out");
        final LSFStdOutputParser parser = new LSFStdOutputParser();
        final LSFJobStatistics stats = parser.getJobStdOutputStatistics(lsfStdOutputFilePath);

        final Date expectedStartDateTime;
        final Date expectedEndDateTime;
        try {
            expectedStartDateTime = new SimpleDateFormat("EEE MMM d hh:mm:ss yyyy").parse("Thu Oct 17 10:37:28 2013");
            expectedEndDateTime = new SimpleDateFormat("EEE MMM d hh:mm:ss yyyy").parse("Thu Oct 17 10:42:53 2013");
        } catch (java.text.ParseException e) {
            Assert.fail("Test in error");
            return;
        }

        Assert.assertNotNull(stats);
        Assert.assertEquals(311847, stats.getJobId());
        Assert.assertEquals(expectedStartDateTime, stats.getStartDateTime());
        Assert.assertEquals(expectedEndDateTime, stats.getEndDateTime());
        Assert.assertEquals(new Double(114.75), stats.getCpuTime());
        Assert.assertEquals(null, stats.getMaxMemory());
        Assert.assertEquals(null, stats.getMaxSwap());
        Assert.assertEquals(null, stats.getMaxProcesses());
        Assert.assertEquals(null, stats.getMaxThreads());
        Assert.assertEquals(true, stats.isOutputComplete());
        Assert.assertEquals(true, stats.isJobSuccessful());
        Assert.assertEquals(true, stats.isOK());
    }
}
