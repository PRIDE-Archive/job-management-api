package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.statistics;

import org.apache.log4j.Logger;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.io.ParseException;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * An object containing summary statistics from running a batch of jobs.
 *
 * @author Matthew Fraser, EMBL-EBI, InterPro
 * @version $Id: LSFJobBatchStatisticsSummary.java,v 1.6 2014/01/15 17:00:06 maxim Exp $
 * @since 1.0-SNAPSHOT
 */
public class LSFJobBatchStatisticsSummary {

    private static final Logger LOGGER = Logger.getLogger(LSFJobBatchStatisticsSummary.class);

    private Long totalRunningTime = null;
    private Long averageRunningTime = null;
    private Long highestRunningTime = null;
    private Long lowestRunningTime = null;

    private Double totalCPUTime = null;
    private Double averageCPUTime = null;
    private Double highestCPUTime = null;
    private Double lowestCPUTime = null;
    private Integer countCPUTime = new Integer(0);

    private Long totalMemoryUse = null;
    private Long averageMemoryUse = null;
    private Long highestMemoryUse = null;
    private Long lowestMemoryUse = null;
    private Integer countMemoryUse = new Integer(0);

    private Long totalSwapUse = null;
    private Long averageSwapUse = null;
    private Long highestSwapUse = null;
    private Long lowestSwapUse = null;
    private Integer countSwapUse = new Integer(0);

    private Integer totalProcesses = null;
    private Integer averageProcesses = null;
    private Integer highestProcesses = null;
    private Integer lowestProcesses = null;
    private Integer countProcesses = new Integer(0);

    private Integer totalThreads = null;
    private Integer averageThreads = null;
    private Integer highestThreads = null;
    private Integer lowestThreads = null;
    private Integer countThreads = new Integer(0);

    // Time from the earliest job start to the latest job finish
    private Date earliestJobStart = null;
    private Date latestJobEnd = null;
    private Long totalBatchWallClockTime = null;

    private int numberOfJobs = 0;
    private Set<Long> completedJobIds = new HashSet<Long>();
    private Boolean successful = null;

    /**
     * Construct the summary of statistics for a batch from each of the individual jobs statistics.
     * @param statsList List of job statistics
     */
    public LSFJobBatchStatisticsSummary(List<LSFJobStatistics> statsList) {
        LOGGER.info("Creating the LSF job batch statistics summary...");
        if (statsList == null || statsList.isEmpty()) {
            this.successful = false;
        }
        else {

            this.numberOfJobs = statsList.size();

            Long secondLowestRunningTime = null;
            Double secondLowestCPUTime = null;
            Long secondLowestMemoryUse = null;
            Long secondLowestSwapUse = null;
            Integer secondLowestProcesses = null;
            Integer secondLowestThreads = null;

            // Loop through the stats for each job and populate the summary figures!
            // Could have tried using list sorting instead, but could be messy since you need to sort by different fields
            // each time!
            for (LSFJobStatistics stats : statsList) {
                final long jobId = stats.getJobId();
                if (stats.isOK()) {
                    completedJobIds.add(jobId);

                    // Job start and end
                    final Date jobStart = stats.getStartDateTime();
                    if (jobStart == null) {
                        throw new ParseException("Job start date was NULL for job " + jobId);
                    }
                    if (earliestJobStart == null || jobStart.before(earliestJobStart)) {
                        earliestJobStart = jobStart;
                    }
                    final Date jobEnd = stats.getEndDateTime();
                    if (jobEnd == null) {
                        throw new ParseException("Job end date was NULL for job " + jobId);
                    }
                    if (latestJobEnd == null || jobEnd.after(latestJobEnd)) {
                        latestJobEnd = jobEnd;
                    }

                    // Running time
                    final long jobRunningTime = (jobEnd.getTime() - jobStart.getTime()) / 1000;
                    if (totalRunningTime == null) {
                        totalRunningTime = new Long(0);
                    }
                    totalRunningTime += jobRunningTime;
                    if (lowestRunningTime == null || jobRunningTime < lowestRunningTime) {
                        // First value available
                        secondLowestRunningTime = lowestRunningTime;
                        lowestRunningTime = jobRunningTime;
                    }
                    else if (secondLowestRunningTime == null) {
                        // Second value available
                        secondLowestRunningTime = jobRunningTime;
                    }
                    if (highestRunningTime == null || jobRunningTime > highestRunningTime) {
                        highestRunningTime = jobRunningTime;
                    }

                    // CPU time
                    final Double jobCPUTime = stats.getCpuTime();
                    if (jobCPUTime != null) {
                        countCPUTime += 1;
                        if (totalCPUTime == null) {
                            totalCPUTime = new Double(0);
                        }
                        totalCPUTime += jobCPUTime;
                        if (lowestCPUTime == null || jobCPUTime < lowestCPUTime) {
                            // First value available
                            secondLowestCPUTime = lowestCPUTime;
                            lowestCPUTime = jobCPUTime;
                        }
                        else if (secondLowestCPUTime == null) {
                            // Second value available
                            secondLowestCPUTime = jobCPUTime;
                        }
                        if (highestCPUTime == null || jobCPUTime > highestCPUTime) {
                            highestCPUTime = jobCPUTime;
                        }
                    }

                    // Memory use (could be NULL)
                    final Long jobMemoryUse = stats.getMaxMemory();
                    if (jobMemoryUse != null) {
                        countMemoryUse += 1;
                        if (totalMemoryUse == null) {
                            totalMemoryUse = new Long(0);
                        }
                        totalMemoryUse += jobMemoryUse;
                        if (lowestMemoryUse == null || jobMemoryUse < lowestMemoryUse) {
                            // First value available
                            secondLowestMemoryUse = lowestMemoryUse;
                            lowestMemoryUse = jobMemoryUse;
                        }
                        else if (secondLowestMemoryUse == null) {
                            // Second value available
                            secondLowestMemoryUse = jobMemoryUse;
                        }
                        if (highestMemoryUse == null || jobMemoryUse > highestMemoryUse) {
                            highestMemoryUse = jobMemoryUse;
                        }
                    }

                    // Swap use (could be NULL)
                    final Long jobSwapUse = stats.getMaxSwap();
                    if (jobSwapUse != null) {
                        countSwapUse += 1;
                        if (totalSwapUse == null) {
                            totalSwapUse = new Long(0);
                        }
                        totalSwapUse += jobSwapUse;
                        if (lowestSwapUse == null || jobSwapUse < lowestSwapUse) {
                            // First value available
                            secondLowestSwapUse = lowestSwapUse;
                            lowestSwapUse = jobSwapUse;
                        }
                        else if (secondLowestSwapUse == null) {
                            // Second value available
                            secondLowestSwapUse = jobSwapUse;
                        }
                        if (highestSwapUse == null || jobSwapUse > highestSwapUse) {
                            highestSwapUse = jobSwapUse;
                        }
                    }

                    // Number of processes (could be NULL)
                    final Integer jobProcesses = stats.getMaxProcesses();
                    if (jobProcesses != null) {
                        countProcesses += 1;
                        if (totalProcesses == null) {
                            totalProcesses = new Integer(0);
                        }
                        totalProcesses += jobProcesses;
                        if (lowestProcesses == null || jobProcesses < lowestProcesses) {
                            // First value available
                            secondLowestProcesses = lowestProcesses;
                            lowestProcesses = jobProcesses;
                        }
                        else if (secondLowestProcesses == null) {
                            // Second value available
                            secondLowestProcesses = jobProcesses;
                        }
                        if (highestProcesses == null || jobProcesses > highestProcesses) {
                            highestProcesses = jobProcesses;
                        }
                    }

                    // Number of threads (could be NULL)
                    final Integer jobThreads = stats.getMaxThreads();
                    if (jobThreads != null) {
                        countThreads += 1;
                        if (totalThreads == null) {
                            totalThreads = new Integer(0);
                        }
                        totalThreads += jobThreads;
                        if (lowestThreads == null || jobThreads < lowestThreads) {
                            // First value available
                            secondLowestThreads = lowestThreads;
                            lowestThreads = jobThreads;
                        }
                        else if (secondLowestThreads == null) {
                            // Second value available
                            secondLowestThreads = jobThreads;
                        }
                        if (highestThreads == null || jobThreads > highestThreads) {
                            highestThreads = jobThreads;
                        }
                    }
                }
            }

            // Now we have all the info, finish constructing the summary object!
            finaliseTheSummary(secondLowestRunningTime, secondLowestCPUTime, secondLowestMemoryUse, secondLowestSwapUse, secondLowestProcesses, secondLowestThreads);
            LOGGER.info("Finished creation of the LSF job batch statistics summary.");
        }
    }

    /**
     * Calculate/tweak any final summary details, for example:
     * - Calculate the average values;
     * - For batches of more than one, remove the smallest value of each type (since typically when chunking an input,
     *   the last chunk will be much smaller than the others and would distort the statistics).
     *
     * @param secondLowestRunningTime
     * @param secondLowestCPUTime
     * @param secondLowestMemoryUse
     * @param secondLowestSwapUse
     * @param secondLowestProcesses
     * @param secondLowestThreads
     */
    private void finaliseTheSummary(Long secondLowestRunningTime, Double secondLowestCPUTime, Long secondLowestMemoryUse, Long secondLowestSwapUse, Integer secondLowestProcesses, Integer secondLowestThreads) {
        int numOfcompletedJobs = completedJobIds.size();
        // Running time
        if (secondLowestRunningTime == null) {
            if (numOfcompletedJobs > 1) {
                throw new ParseException("No second lowest running time found, even though we have more than one completed job");
            }
            averageRunningTime = lowestRunningTime; // Only have one result
        }
        else {
            // Need to throw away the smallest result
            averageRunningTime = Math.round((totalRunningTime - lowestRunningTime) / (numOfcompletedJobs - 1d));
            lowestRunningTime = secondLowestRunningTime;
        }

        // CPU time
        if (secondLowestCPUTime == null) {
            if (countCPUTime > 1) {
                throw new ParseException("No second lowest CPU time found, even though we have more than one job with this data available");
            }
            averageCPUTime = lowestCPUTime; // Only have one result
        }
        else {
            // Need to throw away the smallest result
            BigDecimal bd = new BigDecimal((totalCPUTime - lowestCPUTime) / (countCPUTime - 1));
            bd = bd.setScale(2, RoundingMode.HALF_UP);
            averageCPUTime = bd.doubleValue();
            lowestCPUTime = secondLowestCPUTime;
        }

        // Memory use
        if (secondLowestMemoryUse == null) {
            if (countMemoryUse > 1) {
                throw new ParseException("No second lowest memory usage found, even though we have more than one job with this data available");
            }
            averageMemoryUse = lowestMemoryUse; // Only have one result
        }
        else {
            // Need to throw away the smallest result
            averageMemoryUse = Math.round((totalMemoryUse - lowestMemoryUse) / (countMemoryUse - 1d));
            lowestMemoryUse = secondLowestMemoryUse;
        }

        // Swap use
        if (secondLowestSwapUse == null) {
            if (countSwapUse > 1) {
                throw new ParseException("No second lowest swap usage found, even though we have more than one job wth this data available");
            }
            averageSwapUse = lowestSwapUse; // Only have one result
        }
        else {
            // Need to throw away the smallest result
            averageSwapUse = Math.round((totalSwapUse - lowestSwapUse) / (countSwapUse - 1d));
            lowestSwapUse = secondLowestSwapUse;
        }

        // Number of processes
        if (secondLowestProcesses == null) {
            if (countProcesses > 1) {
                throw new ParseException("No second lowest number of processes found, even though we have more than one job with this data available");
            }
            averageProcesses = lowestProcesses; // Only have one result
        }
        else {
            // Need to throw away the smallest result
            averageProcesses = Math.round((totalProcesses - lowestProcesses) / (countProcesses - 1));
            lowestProcesses = secondLowestProcesses;
        }

        // Number of threads
        if (secondLowestThreads == null) {
            if (countThreads > 1) {
                throw new ParseException("No second lowest number of threads found, even though we have more than one job with this data available");
            }
            averageThreads = lowestThreads; // Only have one result
        }
        else {
            // Need to throw away the smallest result
            averageThreads = Math.round((totalThreads - lowestThreads) / (countThreads - 1));
            lowestThreads = secondLowestThreads;
        }

        totalBatchWallClockTime = (latestJobEnd.getTime() - earliestJobStart.getTime()) / 1000;

        if (numOfcompletedJobs == numberOfJobs) {
            successful = true;
        }
        else {
            successful = false;
        }
    }

    public Double getTotalCPUTime() {
        return totalCPUTime;
    }

    public Double getAverageCPUTime() {
        return averageCPUTime;
    }

    public Double getHighestCPUTime() {
        return highestCPUTime;
    }

    public Double getLowestCPUTime() {
        return lowestCPUTime;
    }

    public Integer getCountCPUTime() {
        return countCPUTime;
    }

    public Long getTotalRunningTime() {
        return totalRunningTime;
    }

    public Long getAverageRunningTime() {
        return averageRunningTime;
    }

    public Long getHighestRunningTime() {
        return highestRunningTime;
    }

    public Long getLowestRunningTime() {
        return lowestRunningTime;
    }

    public Long getTotalMemoryUse() {
        return totalMemoryUse;
    }

    public Long getAverageMemoryUse() {
        return averageMemoryUse;
    }

    public Long getHighestMemoryUse() {
        return highestMemoryUse;
    }

    public Long getLowestMemoryUse() {
        return lowestMemoryUse;
    }

    public Integer getCountMemoryUse() {
        return countMemoryUse;
    }

    public Long getTotalSwapUse() {
        return totalSwapUse;
    }

    public Long getAverageSwapUse() {
        return averageSwapUse;
    }

    public Long getHighestSwapUse() {
        return highestSwapUse;
    }

    public Long getLowestSwapUse() {
        return lowestSwapUse;
    }

    public Integer getCountSwapUse() {
        return countSwapUse;
    }

    public Integer getTotalProcesses() {
        return totalProcesses;
    }

    public Integer getAverageProcesses() {
        return averageProcesses;
    }

    public Integer getHighestProcesses() {
        return highestProcesses;
    }

    public Integer getLowestProcesses() {
        return lowestProcesses;
    }

    public Integer getCountProcesses() {
        return countProcesses;
    }

    public Integer getTotalThreads() {
        return totalThreads;
    }

    public Integer getAverageThreads() {
        return averageThreads;
    }

    public Integer getHighestThreads() {
        return highestThreads;
    }

    public Integer getLowestThreads() {
        return lowestThreads;
    }

    public Integer getCountThreads() {
        return countThreads;
    }

    public Date getEarliestJobStart() {
        return earliestJobStart;
    }

    public Date getLatestJobEnd() {
        return latestJobEnd;
    }

    public Long getTotalBatchWallClockTime() {
        return totalBatchWallClockTime;
    }

    public int getNumberOfJobs() {
        return numberOfJobs;
    }

    public Set<Long> getCompletedJobIds() {
        return completedJobIds;
    }

    public Boolean isAllSuccessful() {
        return successful;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder()
                .append("LSFJobBatchStatisticsSummary:\n");
        if (totalRunningTime != null) {
            sb.append("\nTotal running time = ").append(totalRunningTime).append("s");
        }
        if (averageRunningTime != null) {
            sb.append("\nAverage running time = ").append(averageRunningTime).append("s");
        }
        if (highestRunningTime != null) {
            sb.append("\nHighest running time = ").append(highestRunningTime).append("s");
        }
        if (lowestRunningTime != null) {
            sb.append("\nLowest running time = ").append(lowestRunningTime).append("s");
        }
        if (totalCPUTime != null) {
            sb.append("\nTotal CPU time = ").append(totalCPUTime).append("s");
        }
        if (averageCPUTime != null) {
            sb.append("\nAverage CPU time = ").append(averageCPUTime).append("s");
        }
        if (highestCPUTime != null) {
            sb.append("\nHighest CPU time = ").append(highestCPUTime).append("s");
        }
        if (lowestCPUTime != null) {
            sb.append("\nLowest CPU time = ").append(lowestCPUTime).append("s");
        }
        sb.append("\nNumber of jobs with CPU time reported = ").append(countCPUTime);
        if (totalMemoryUse != null) {
            sb.append("\nTotal memory use = ").append(totalMemoryUse).append("MB");
        }
        if (averageMemoryUse != null) {
            sb.append("\nAverage job memory use = ").append(averageMemoryUse).append("MB");
        }
        if (highestMemoryUse != null) {
            sb.append("\nHighest job memory use = ").append(highestMemoryUse).append("MB");
        }
        if (lowestMemoryUse != null) {
            sb.append("\nLowest job memory use = ").append(lowestMemoryUse).append("MB");
        }
        sb.append("\nNumber of jobs with memory use reported = ").append(countMemoryUse);
        if (totalSwapUse != null) {
            sb.append("\nTotal swap use = ").append(totalSwapUse).append("MB");
        }
        if (averageSwapUse != null) {
            sb.append("\nAverage job swap use = ").append(averageSwapUse).append("MB");
        }
        if (highestSwapUse != null) {
            sb.append("\nHighest job swap use = ").append(highestSwapUse).append("MB");
        }
        if (lowestSwapUse != null) {
            sb.append("\nLowest job swap use = ").append(lowestSwapUse).append("MB");
        }
        sb.append("\nNumber of jobs with swap use reported = ").append(countSwapUse);
        if (totalProcesses != null) {
            sb.append("\nTotal number of processes = ").append(totalProcesses);
        }
        if (averageProcesses != null) {
            sb.append("\nAverage number of processes = ").append(averageProcesses);
        }
        if (highestProcesses != null) {
            sb.append("\nHighest number of processes = ").append(highestProcesses);
        }
        if (lowestProcesses != null) {
            sb.append("\nLowest number of processes = ").append(lowestProcesses);
        }
        sb.append("\nNumber of jobs with number of processes reported = ").append(countProcesses);
        if (totalThreads != null) {
            sb.append("\nTotal number of threads = ").append(totalThreads);
        }
        if (averageThreads != null) {
            sb.append("\nAverage number of threads = ").append(averageThreads);
        }
        if (highestThreads != null) {
            sb.append("\nHighest number of threads = ").append(highestThreads);
        }
        if (lowestThreads != null) {
            sb.append("\nLowest number of threads = ").append(lowestThreads);
        }
        sb.append("\nNumber of jobs with number of threads reported = ").append(countThreads);
        if (earliestJobStart != null) {
            sb.append("\nEarliest job start = ").append(earliestJobStart);
        }
        if (latestJobEnd != null) {
            sb.append("\nlatest job end = ").append(latestJobEnd);
        }
        if (totalBatchWallClockTime != null) {
            sb.append("\nTotal wall clock time for the whole batch = ").append(totalBatchWallClockTime).append("s");
        }
        sb.append("\nNumber of jobs = ").append(numberOfJobs);
        if (completedJobIds != null && !completedJobIds.isEmpty()) {
            sb.append("\nCompleted job ids = ");
            boolean first = true;
            for (Long jobId : completedJobIds) {
                if (first) {
                    sb.append(jobId);
                    first = false;
                }
                else {
                    sb.append(", ").append(jobId);
                }
            }
        }
        sb.append("\nWas whole batch successful = ").append(successful);
        return sb.toString();
    }
}
