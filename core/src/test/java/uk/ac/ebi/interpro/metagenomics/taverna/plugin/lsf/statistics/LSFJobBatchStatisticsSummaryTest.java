package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.statistics;

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.BaseTestClass;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.io.LSFStdOutputParser;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.io.ParseException;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Check a list of LSF standard output text can be parsed and the statistics summarised.
 *
 * @author Matthew Fraser, EMBL-EBI, InterPro
 * @version $Id: LSFJobBatchStatisticsSummaryTest.java,v 1.6 2013/11/25 09:42:26 craigm Exp $
 * @since 1.0-SNAPSHOT
 */
public class LSFJobBatchStatisticsSummaryTest extends BaseTestClass {

    private static final Logger LOGGER = Logger.getLogger(LSFJobBatchStatisticsSummaryTest.class.getName());

    final LSFStdOutputParser parser = new LSFStdOutputParser();

    @Test
    public void summariseBatchStatistics() {

        // Prepare statistics list
        List<LSFJobStatistics> statsList = new ArrayList<LSFJobStatistics>();
        for (int i = 1; i <= 7; i++) {
            final String lsfStdOutputFilePath = getResourceFilePath("batch/bsub_factorial_" + i + ".out");
            LSFJobStatistics stats;
            try {
                stats = parser.getJobStdOutputStatistics(lsfStdOutputFilePath);
            }
            catch (ParseException e) {
                continue;
            }
            Assert.assertNotNull(stats);
            statsList.add(stats);
        }

        final LSFJobBatchStatisticsSummary summary = new LSFJobBatchStatisticsSummary(statsList);
        Assert.assertNotNull(summary);
        Assert.assertEquals(statsList.size(), summary.getNumberOfJobs());
        Assert.assertEquals(5, summary.getCompletedJobIds().size());
        final Set<Long> completedJobIds = new HashSet<Long>(Arrays.asList(311845L, 313357L, 314140L, 311847L, 311848L));
        Assert.assertEquals(completedJobIds, summary.getCompletedJobIds());

        final Date expectedEarliestStart;
        final Date expectedLatestEnd;
        final long expectedWallClockTime;
        try {
            expectedEarliestStart = new SimpleDateFormat("EEE MMM d hh:mm:ss yyyy").parse("Thu Oct 17 10:37:28 2013");
            expectedLatestEnd = new SimpleDateFormat("EEE MMM d hh:mm:ss yyyy").parse("Thu Oct 17 10:45:41 2013");
            expectedWallClockTime = (expectedLatestEnd.getTime() - expectedEarliestStart.getTime()) / 1000;
        } catch (java.text.ParseException e) {
            Assert.fail("Test in error");
            return;
        }

        Assert.assertEquals(expectedEarliestStart, summary.getEarliestJobStart());
        Assert.assertEquals(expectedLatestEnd, summary.getLatestJobEnd());
        Assert.assertEquals(new Long(expectedWallClockTime), summary.getTotalBatchWallClockTime());

        try {
            // Code to calculate the job running times
            long tmp1 = new SimpleDateFormat("EEE MMM d hh:mm:ss yyyy").parse("Thu Oct 17 10:42:53 2013").getTime() - new SimpleDateFormat("EEE MMM d hh:mm:ss yyyy").parse("Thu Oct 17 10:37:28 2013").getTime();
            long tmp2 = new SimpleDateFormat("EEE MMM d hh:mm:ss yyyy").parse("Thu Oct 17 10:43:40 2013").getTime() - new SimpleDateFormat("EEE MMM d hh:mm:ss yyyy").parse("Thu Oct 17 10:38:38 2013").getTime();
            long tmp3 = new SimpleDateFormat("EEE MMM d hh:mm:ss yyyy").parse("Thu Oct 17 10:45:41 2013").getTime() - new SimpleDateFormat("EEE MMM d hh:mm:ss yyyy").parse("Thu Oct 17 10:40:13 2013").getTime();
            long tmp6 = new SimpleDateFormat("EEE MMM d hh:mm:ss yyyy").parse("Thu Oct 17 10:42:53 2013").getTime() - new SimpleDateFormat("EEE MMM d hh:mm:ss yyyy").parse("Thu Oct 17 10:37:28 2013").getTime();
            long tmp7 = new SimpleDateFormat("EEE MMM d hh:mm:ss yyyy").parse("Thu Oct 17 10:42:53 2013").getTime() - new SimpleDateFormat("EEE MMM d hh:mm:ss yyyy").parse("Thu Oct 17 10:37:28 2013").getTime();
        } catch (java.text.ParseException e) {
            Assert.fail("Test in error");
        }

        Assert.assertEquals(new Double(573.94), summary.getTotalCPUTime()); // 114.75 + 101.38 + 128.31 + 114.75 + 114.75
        Assert.assertEquals(new Double(118.14), summary.getAverageCPUTime());
        Assert.assertEquals(new Double(114.75), summary.getLowestCPUTime());
        Assert.assertEquals(new Double(128.31), summary.getHighestCPUTime());
        Assert.assertEquals(new Integer(5), summary.getCountCPUTime());

        Assert.assertEquals(new Long(1605), summary.getTotalRunningTime()); // 325 + 302 + 328 + 325 + 325
        Assert.assertEquals(new Long(326), summary.getAverageRunningTime());
        Assert.assertEquals(new Long(325), summary.getLowestRunningTime());
        Assert.assertEquals(new Long(328), summary.getHighestRunningTime());

        Assert.assertEquals(new Long(44), summary.getTotalMemoryUse()); // 11 + 11 + 11 + 11
        Assert.assertEquals(new Long(11), summary.getAverageMemoryUse());
        Assert.assertEquals(new Long(11), summary.getLowestMemoryUse());
        Assert.assertEquals(new Long(11), summary.getHighestMemoryUse());
        Assert.assertEquals(new Integer(4), summary.getCountMemoryUse());

        Assert.assertEquals(new Long(714), summary.getTotalSwapUse()); // 237 + 237 + 240
        Assert.assertEquals(new Long(239), summary.getAverageSwapUse());
        Assert.assertEquals(new Long(237), summary.getLowestSwapUse());
        Assert.assertEquals(new Long(240), summary.getHighestSwapUse());
        Assert.assertEquals(new Integer(3), summary.getCountSwapUse());

        Assert.assertEquals(new Integer(9), summary.getTotalProcesses()); // 3 + 3 + 3
        Assert.assertEquals(new Integer(3), summary.getAverageProcesses());
        Assert.assertEquals(new Integer(3), summary.getLowestProcesses());
        Assert.assertEquals(new Integer(3), summary.getHighestProcesses());
        Assert.assertEquals(new Integer(3), summary.getCountProcesses());

        Assert.assertEquals(new Integer(12), summary.getTotalThreads()); // 4 + 4 + 4
        Assert.assertEquals(new Integer(4), summary.getAverageThreads());
        Assert.assertEquals(new Integer(4), summary.getLowestThreads());
        Assert.assertEquals(new Integer(4), summary.getHighestThreads());
        Assert.assertEquals(new Integer(3), summary.getCountThreads());

        Assert.assertFalse(summary.isAllSuccessful()); // One of the batch job outputs was incomplete

        String toStr = summary.toString();
        Assert.assertNotNull(toStr);
    }

    @Test
    public void summariseNoStatistics() {
        LSFJobBatchStatisticsSummary summary = new LSFJobBatchStatisticsSummary(null);
        Assert.assertNotNull(summary);
        Assert.assertFalse(summary.isAllSuccessful());
        String toStr = summary.toString();
        Assert.assertNotNull(toStr);
    }

    @Test
    public void summariseBatchOfOneStatistics() {

        // Prepare statistics list
        List<LSFJobStatistics> statsList = new ArrayList<LSFJobStatistics>();
        final String lsfStdOutputFilePath = getResourceFilePath("batch/bsub_factorial_7.out");
        LSFJobStatistics stats;
        try {
            stats = parser.getJobStdOutputStatistics(lsfStdOutputFilePath);
        }
        catch (ParseException e) {
            Assert.fail("Test in error");
            return;
        }
        Assert.assertNotNull(stats);
        statsList.add(stats);

        LSFJobBatchStatisticsSummary summary = new LSFJobBatchStatisticsSummary(statsList);
        Assert.assertNotNull(summary);
        Assert.assertEquals(statsList.size(), summary.getNumberOfJobs());
        Assert.assertEquals(statsList.size(), summary.getCompletedJobIds().size());

        final Date expectedEarliestStart;
        final Date expectedLatestEnd;
        final long expectedWallClockTime;
        try {
            expectedEarliestStart = new SimpleDateFormat("EEE MMM d hh:mm:ss yyyy").parse("Thu Oct 17 10:37:28 2013");
            expectedLatestEnd = new SimpleDateFormat("EEE MMM d hh:mm:ss yyyy").parse("Thu Oct 17 10:42:53 2013");
            expectedWallClockTime = (expectedLatestEnd.getTime() - expectedEarliestStart.getTime()) / 1000;
        } catch (java.text.ParseException e) {
            Assert.fail("Test in error");
            return;
        }

        Assert.assertEquals(expectedEarliestStart, summary.getEarliestJobStart());
        Assert.assertEquals(expectedLatestEnd, summary.getLatestJobEnd());
        Assert.assertEquals(new Long(expectedWallClockTime), summary.getTotalBatchWallClockTime());

        try {
            // Code to calculate the job running times
            long tmp6 = new SimpleDateFormat("EEE MMM d hh:mm:ss yyyy").parse("Thu Oct 17 10:42:53 2013").getTime() - new SimpleDateFormat("EEE MMM d hh:mm:ss yyyy").parse("Thu Oct 17 10:37:28 2013").getTime();
        } catch (java.text.ParseException e) {
            Assert.fail("Test in error");
        }

        Assert.assertEquals(new Double(114.75), summary.getTotalCPUTime()); // 114.75
        Assert.assertEquals(new Double(114.75), summary.getAverageCPUTime());
        Assert.assertEquals(new Double(114.75), summary.getLowestCPUTime());
        Assert.assertEquals(new Double(114.75), summary.getHighestCPUTime());
        Assert.assertEquals(new Integer(1), summary.getCountCPUTime());

        Assert.assertEquals(new Long(325), summary.getTotalRunningTime()); // 325
        Assert.assertEquals(new Long(325), summary.getAverageRunningTime());
        Assert.assertEquals(new Long(325), summary.getLowestRunningTime());
        Assert.assertEquals(new Long(325), summary.getHighestRunningTime());

        Assert.assertEquals(new Long(11), summary.getTotalMemoryUse()); // 11
        Assert.assertEquals(new Long(11), summary.getAverageMemoryUse());
        Assert.assertEquals(new Long(11), summary.getLowestMemoryUse());
        Assert.assertEquals(new Long(11), summary.getHighestMemoryUse());
        Assert.assertEquals(new Integer(1), summary.getCountMemoryUse());

        Assert.assertEquals(null, summary.getTotalSwapUse()); // NULL
        Assert.assertEquals(null, summary.getAverageSwapUse());
        Assert.assertEquals(null, summary.getLowestSwapUse());
        Assert.assertEquals(null, summary.getHighestSwapUse());
        Assert.assertEquals(new Integer(0), summary.getCountSwapUse());

        Assert.assertEquals(null, summary.getTotalProcesses()); // NULL
        Assert.assertEquals(null, summary.getAverageProcesses());
        Assert.assertEquals(null, summary.getLowestProcesses());
        Assert.assertEquals(null, summary.getHighestProcesses());
        Assert.assertEquals(new Integer(0), summary.getCountProcesses());

        Assert.assertEquals(null, summary.getTotalThreads()); // NULL
        Assert.assertEquals(null, summary.getAverageThreads());
        Assert.assertEquals(null, summary.getLowestThreads());
        Assert.assertEquals(null, summary.getHighestThreads());
        Assert.assertEquals(new Integer(0), summary.getCountThreads());

        Assert.assertTrue(summary.isAllSuccessful());

        String toStr = summary.toString();
        Assert.assertNotNull(toStr);
    }

    @Test
    public void summariseBatchOfTwoStatistics() {
        // Prepare statistics list
        List<LSFJobStatistics> statsList = new ArrayList<LSFJobStatistics>();
        for (int i = 1; i <= 2; i++) {
            final String lsfStdOutputFilePath = getResourceFilePath("batch/bsub_factorial_" + i + ".out");
            LSFJobStatistics stats;
            try {
                stats = parser.getJobStdOutputStatistics(lsfStdOutputFilePath);
            }
            catch (ParseException e) {
                continue;
            }
            Assert.assertNotNull(stats);
            statsList.add(stats);
        }

        LSFJobBatchStatisticsSummary summary = new LSFJobBatchStatisticsSummary(statsList);
        Assert.assertNotNull(summary);
        Assert.assertEquals(statsList.size(), summary.getNumberOfJobs());
        Assert.assertEquals(statsList.size(), summary.getCompletedJobIds().size());

        final Date expectedEarliestStart;
        final Date expectedLatestEnd;
        final long expectedWallClockTime;
        try {
            expectedEarliestStart = new SimpleDateFormat("EEE MMM d hh:mm:ss yyyy").parse("Thu Oct 17 10:37:28 2013");
            expectedLatestEnd = new SimpleDateFormat("EEE MMM d hh:mm:ss yyyy").parse("Thu Oct 17 10:43:40 2013");
            expectedWallClockTime = (expectedLatestEnd.getTime() - expectedEarliestStart.getTime()) / 1000;
        } catch (java.text.ParseException e) {
            Assert.fail("Test in error");
            return;
        }

        Assert.assertEquals(expectedEarliestStart, summary.getEarliestJobStart());
        Assert.assertEquals(expectedLatestEnd, summary.getLatestJobEnd());
        Assert.assertEquals(new Long(expectedWallClockTime), summary.getTotalBatchWallClockTime());

        try {
            // Code to calculate the job running times
            long tmp1 = new SimpleDateFormat("EEE MMM d hh:mm:ss yyyy").parse("Thu Oct 17 10:42:53 2013").getTime() - new SimpleDateFormat("EEE MMM d hh:mm:ss yyyy").parse("Thu Oct 17 10:37:28 2013").getTime();
            long tmp2 = new SimpleDateFormat("EEE MMM d hh:mm:ss yyyy").parse("Thu Oct 17 10:43:40 2013").getTime() - new SimpleDateFormat("EEE MMM d hh:mm:ss yyyy").parse("Thu Oct 17 10:38:38 2013").getTime();
        } catch (java.text.ParseException e) {
            Assert.fail("Test in error");
        }

        Assert.assertEquals(new Double(216.13), summary.getTotalCPUTime()); // 114.75 + 101.38
        Assert.assertEquals(new Double(114.75), summary.getAverageCPUTime());
        Assert.assertEquals(new Double(114.75), summary.getLowestCPUTime());
        Assert.assertEquals(new Double(114.75), summary.getHighestCPUTime());
        Assert.assertEquals(new Integer(2), summary.getCountCPUTime());

        Assert.assertEquals(new Long(627), summary.getTotalRunningTime()); // 325 + 302
        Assert.assertEquals(new Long(325), summary.getAverageRunningTime());
        Assert.assertEquals(new Long(325), summary.getLowestRunningTime());
        Assert.assertEquals(new Long(325), summary.getHighestRunningTime());

        Assert.assertEquals(new Long(22), summary.getTotalMemoryUse()); // 11 + 11
        Assert.assertEquals(new Long(11), summary.getAverageMemoryUse());
        Assert.assertEquals(new Long(11), summary.getLowestMemoryUse());
        Assert.assertEquals(new Long(11), summary.getHighestMemoryUse());
        Assert.assertEquals(new Integer(2), summary.getCountMemoryUse());

        Assert.assertEquals(new Long(474), summary.getTotalSwapUse()); // 237 + 237
        Assert.assertEquals(new Long(237), summary.getAverageSwapUse());
        Assert.assertEquals(new Long(237), summary.getLowestSwapUse());
        Assert.assertEquals(new Long(237), summary.getHighestSwapUse());
        Assert.assertEquals(new Integer(2), summary.getCountSwapUse());

        Assert.assertEquals(new Integer(6), summary.getTotalProcesses()); // 3 + 3
        Assert.assertEquals(new Integer(3), summary.getAverageProcesses());
        Assert.assertEquals(new Integer(3), summary.getLowestProcesses());
        Assert.assertEquals(new Integer(3), summary.getHighestProcesses());
        Assert.assertEquals(new Integer(2), summary.getCountProcesses());

        Assert.assertEquals(new Integer(8), summary.getTotalThreads()); // 4 + 4
        Assert.assertEquals(new Integer(4), summary.getAverageThreads());
        Assert.assertEquals(new Integer(4), summary.getLowestThreads());
        Assert.assertEquals(new Integer(4), summary.getHighestThreads());
        Assert.assertEquals(new Integer(2), summary.getCountThreads());

        Assert.assertTrue(summary.isAllSuccessful());

        String toStr = summary.toString();
        Assert.assertNotNull(toStr);
    }

}
