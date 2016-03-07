package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.statistics;

import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * Statistic associated with a job as found by parsing the LSF output text.
 *
 * @author Matthew Fraser, EMBL-EBI, InterPro
 * @version $Id: LSFJobStatistics.java,v 1.3 2014/05/30 11:45:55 craigm Exp $
 * @since 1.0-SNAPSHOT
 */
public class LSFJobStatistics {

    private Long jobId;
    private Date startDateTime;
    private Date endDateTime;
    private Double cpuTime;
    private Long maxMemory;
    private Long maxSwap;
    private Integer maxProcesses;
    private Integer maxThreads;
    private boolean isJobSuccessful;
    private boolean isOutputComplete;

    public LSFJobStatistics(Long jobId, Date startDateTime, Date endDateTime, Double cpuTime, Long maxMemory, Long maxSwap, Integer maxProcesses, Integer maxThreads, boolean jobSuccessful, boolean outputComplete) {
        this.jobId = jobId;
        this.startDateTime = startDateTime;
        this.endDateTime = endDateTime;
        this.cpuTime = cpuTime;
        this.maxMemory = maxMemory;
        this.maxSwap = maxSwap;
        this.maxProcesses = maxProcesses;
        this.maxThreads = maxThreads;
        this.isJobSuccessful = jobSuccessful;
        this.isOutputComplete = outputComplete;
    }

    /**
     * Sort the list using the supplied sort field.
     * @param statsList The list to sort
     * @param sortField The field to sort by
     * @param ascending If true sort in ascending order, otherwise use descending order
     */
    public static void sortList(List statsList, String sortField, boolean ascending){

        // TODO Note: This method isn't actually used in the project. But not deleted since it might be useful one day!

        if("StartDateTime".equals(sortField)){
            Collections.sort(statsList, new ReflectionComparator("getStartDateTime"));
        }
        else if("EndDateTime".equals(sortField)){
            Collections.sort(statsList, new ReflectionComparator("getEndDateTime"));
        }
        else if("CpuTime".equals(sortField)){
            Collections.sort(statsList, new ReflectionComparator("getCpuTime"));
        }
        else if("MaxMemory".equals(sortField)){
            Collections.sort(statsList, new ReflectionComparator("getMaxMemory"));
        }
        else if("MaxSwap".equals(sortField)){
            Collections.sort(statsList, new ReflectionComparator("getMaxSwap"));
        }
        else if("MaxProcesses".equals(sortField)){
            Collections.sort(statsList, new ReflectionComparator("getMaxProcesses"));
        }
        else if("MaxThreads".equals(sortField)){
            Collections.sort(statsList, new ReflectionComparator("getMaxThreads"));
        }
        if(!ascending){
            Collections.reverse(statsList);
        }

    }


    public long getJobId() {
        return jobId;
    }

    public Date getStartDateTime() {
        return startDateTime;
    }

    public Date getEndDateTime() {
        return endDateTime;
    }

    public Double getCpuTime() {
        return cpuTime;
    }

    public Long getMaxMemory() {
        return maxMemory;
    }

    public Long getMaxSwap() {
        return maxSwap;
    }

    public Integer getMaxProcesses() {
        return maxProcesses;
    }

    public Integer getMaxThreads() {
        return maxThreads;
    }

    public boolean isJobSuccessful() {
        return isJobSuccessful;
    }

    public boolean isOutputComplete() {
        return isOutputComplete;
    }

    /**
     * Did the job complete successfully and the output is all present?
     * @return True if all is OK, otherwise false
     */
    public boolean isOK() {
        return isJobSuccessful && isOutputComplete;
    }
}
