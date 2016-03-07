package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.io;

import uk.ac.ebi.interpro.metagenomics.jobManager.IJobOption;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.LSFBsubCommand;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.LSFBsubOption;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.LSFJob;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.statistics.LSFJobStatistics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: craigm
 * Date: 14/08/12
 * Time: 17:53
 * To change this template use File | Settings | File Templates.
 */
public class LSFStdOutputParser {
    private static final String START_MESSAGE = "Sender: LSF System";
    private static final String JOB_ID_MESSAGE = "Subject: Job";
    private static final String SUCCESS_MESSAGE = "Successfully completed";
    private static final String COMPLETE_MESSAGE = "The output (if any) is";
    private static final String START_DATE_MESSAGE = "Started at ";
    private static final String END_DATE_MESSAGE = "Results reported at ";
    private static final String CPU_TIME_MESSAGE = "CPU time";
    private static final String MAX_MEMORY_MESSAGE = "Max Memory";
    private static final String MAX_SWAP_MESSAGE = "Max Swap";
    private static final String MAX_PROCESSES_MESSAGE = "Max Processes";
    private static final String MAX_THREADS_MESSAGE = "Max Threads";



    public LSFJobStatistics getJobStdOutputStatistics(LSFJob lsfJob) {
        LSFBsubCommand bsubCommand = lsfJob.getJobSchedulerCommand();
        String lsfStdOutputFilePath = (String) bsubCommand.getOptionValue(LSFBsubOption.STD_OUTPUT_FILE);
        return getJobStdOutputStatistics(lsfStdOutputFilePath, false);

    }

    public LSFJobStatistics getJobStdOutputStatistics(String fileName) {
        return getJobStdOutputStatistics(fileName, true);
    }

    public LSFJobStatistics getJobStdOutputStatistics(String fileName, boolean strictParsing) {
        return getJobStdOutputStatistics(new File(fileName), strictParsing);
    }

    public LSFJobStatistics getJobStdOutputStatistics(File inFile) {
        return getJobStdOutputStatistics(inFile, true);
    }

    public LSFJobStatistics getJobStdOutputStatistics(File inFile, boolean strictParsing) throws ParseException {
        if (!inFile.exists()) {
            return null;
        }
        boolean isSuccessful = false;
        boolean isComplete = false;
        Long jobId = null;
        Date startDate = null;
        Date endDate = null;
        Double cpuTime = null;
        Long maxMemory = null;
        Long maxSwap = null;
        Integer maxProcesses = null;
        Integer maxThreads = null;

        BufferedReader reader = null;
        LSFJobStatistics jobStats = null; // Used to store the last complete successful job attempt from the LSF output

        try {
            reader = new BufferedReader(new FileReader(inFile));
            String line;
            boolean firstJobAttempt = true;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                else if (line.indexOf(START_MESSAGE) != -1) {
                    // Backup last job statistics just processed in the LSF output
                    if (!firstJobAttempt && isSuccessful && isComplete) {
                        jobStats = new LSFJobStatistics(jobId,
                                startDate,
                                endDate,
                                cpuTime,
                                maxMemory,
                                maxSwap,
                                maxProcesses,
                                maxThreads,
                                isSuccessful,
                                isComplete);
                    }
                    firstJobAttempt = false;

                    // LSF output files are appended to when a new job attempt is run
                    // so reset everything when we come to a new message
                    jobId = null;
                    isSuccessful = false;
                    isComplete = false;
                    startDate = null;
                    endDate = null;
                    cpuTime = null;
                    maxMemory = null;
                    maxSwap = null;
                    maxProcesses = null;
                    maxThreads = null;
                }
                else if (line.indexOf(SUCCESS_MESSAGE) != -1) {
                    // Job was successful
                    isSuccessful = true;
                }
                else if (line.indexOf(COMPLETE_MESSAGE) != -1 && isSuccessful) {
                    // A success message followed by the line indicating message completeness
                    // means that the particular job attempt was successful
                    isComplete = true;
                    if (isSuccessful) {
                        jobStats = new LSFJobStatistics(jobId,
                                startDate,
                                endDate,
                                cpuTime,
                                maxMemory,
                                maxSwap,
                                maxProcesses,
                                maxThreads,
                                isSuccessful,
                                isComplete);
                    }
                }
                else if (line.indexOf(JOB_ID_MESSAGE) != -1) {
                    // If the jobId has not already been populated then populate it now! Example line:
                    // Subject: Job 984631: <UPI0000134D69_UPI0000135151_UPI00004FA907_UPI00004FD017> Done
                    final Pattern pattern = Pattern.compile(JOB_ID_MESSAGE + "\\s+(\\d+):.*$");
                    final Matcher matcher = pattern.matcher(line);
                    if (matcher.find()) {
                        try {
                            jobId = Long.parseLong(matcher.group(1));
                        }
                        catch (NumberFormatException e) {
                            throw new ParseException("Could not convert the job Id into a number.", line);
                        }
                    }
                    else {
                        throw new ParseException("Could not find a Job ID to convert into a number.", line);
                    }
                }
                else if (line.indexOf(START_DATE_MESSAGE) != -1) {
                    // Job start date/time. Example line:
                    // Started at Mon Mar 13 11:32:18 2006
                    line = line.replaceAll(START_DATE_MESSAGE, "").trim();
                    try {
                        startDate = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy").parse(line);
                    } catch (java.text.ParseException e) {
                        throw new ParseException("Could not convert the start date into a date object. Expected format = EEE MMM d hh:mm:ss yyyy.", line);
                    }
                }
                else if (line.indexOf(END_DATE_MESSAGE) != -1) {
                    // Job start date/time. Example line:
                    // Results reported at Mon Mar 13 11:42:30 2006
                    line = line.replaceAll(END_DATE_MESSAGE, "").trim();
                    try {
                        endDate = new SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy").parse(line);
                    } catch (java.text.ParseException e) {
                        throw new ParseException("Could not convert the end date into a date object. Expected format = EEE MMM d hh:mm:ss yyyy.", line);
                    }
                }
                else if (line.indexOf(CPU_TIME_MESSAGE) != -1) {
                    // Job CPU time. Example line:
                    // CPU time   :    412.57 sec.
                    final Pattern pattern = Pattern.compile(CPU_TIME_MESSAGE + "\\s+:\\s+([0-9][0-9.]*)\\s+.*$");
                    final Matcher matcher = pattern.matcher(line);
                    if (matcher.find()) {
                        try {
                            cpuTime = Double.parseDouble(matcher.group(1));
                        }
                        catch (NumberFormatException e) {
                            throw new ParseException("Could not convert the CPU time into a number.", line);
                        }
                    }
                    else {
                        throw new ParseException("Could not find a CPU time to convert into a number.", line);
                    }
                }
                else if (line.indexOf(MAX_MEMORY_MESSAGE) != -1) {
                    // Job maximum memory. Example line:
                    // Max Memory :       123 MB
                    final Pattern pattern = Pattern.compile(MAX_MEMORY_MESSAGE + "\\s+:\\s+(\\d+)\\s+.*$");
                    final Matcher matcher = pattern.matcher(line);
                    if (matcher.find()) {
                        try {
                            maxMemory = Long.parseLong(matcher.group(1));
                        }
                        catch (NumberFormatException e) {
                            throw new ParseException("Could not convert the maximum memory into a number.", line);
                        }
                    }
                    else {
                        throw new ParseException("Could not find a Cmaximum memory to convert into a number.", line);
                    }
                }
                else if (line.indexOf(MAX_SWAP_MESSAGE) != -1) {
                    // Job maximum swap. Example line:
                    // Max Swap   :      1288 MB
                    final Pattern pattern = Pattern.compile(MAX_SWAP_MESSAGE + "\\s+:\\s+(\\d+)\\s+.*$");
                    final Matcher matcher = pattern.matcher(line);
                    if (matcher.find()) {
                        try {
                            maxSwap = Long.parseLong(matcher.group(1));
                        }
                        catch (NumberFormatException e) {
                            throw new ParseException("Could not convert the maximum swap into a number.", line);
                        }
                    }
                    else {
                        throw new ParseException("Could not find a maximum swap to convert into a number.", line);
                    }
                }
                else if (line.indexOf(MAX_PROCESSES_MESSAGE) != -1) {
                    // Job maximum processes. Example line:
                    // Max Processes  :         4
                    final Pattern pattern = Pattern.compile(MAX_PROCESSES_MESSAGE + "\\s+:\\s+(\\d+)$");
                    final Matcher matcher = pattern.matcher(line);
                    if (matcher.find()) {
                        try {
                            maxProcesses = Integer.parseInt(matcher.group(1));
                        }
                        catch (NumberFormatException e) {
                            throw new ParseException("Could not convert the maximum number of processes into a number.", line);
                        }
                    }
                    else {
                        throw new ParseException("Could not find a maximum number of processes to convert into a number.", line);
                    }
                }
                else if (line.indexOf(MAX_THREADS_MESSAGE) != -1) {
                    // Job maximum threads. Example line:
                    // Max Threads  :         4
                    final Pattern pattern = Pattern.compile(MAX_THREADS_MESSAGE + "\\s+:\\s+(\\d+)$");
                    final Matcher matcher = pattern.matcher(line);
                    if (matcher.find()) {
                        try {
                            maxThreads = Integer.parseInt(matcher.group(1));
                        }
                        catch (NumberFormatException e) {
                            throw new ParseException("Could not convert the maximum number of threads into a number.", line);
                        }
                    }
                    else {
                        throw new ParseException("Could not find a maximum number of threads to convert into a number.", line);
                    }
                }
            }
        }  catch (IOException e) {
            throw new IllegalStateException("Problem reading LSF output file: " + inFile, e);
        }  finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    throw new IllegalStateException("Could not close LSF output file: " + inFile, e);
                }
            }
        }

        if (strictParsing) {

            if (jobId == null) {
                throw new ParseException("Mandatory data could not be found, unknown job ID");
            }
            else if (startDate == null || endDate == null) {
                throw new ParseException("Mandatory date data could not be found for job ID " + jobId);
            }
        }

        if (jobStats != null && (!isSuccessful || !isComplete)) {
            // The final job attempt failed, but a previous one succeeded therefore use those stats instead. With
            // speculative execution a job attempt may be killed and therefore the last attempt to be logged may not
            // be the complete successful one!
            return jobStats;
        }

        // Return statistics for the last job attempt (whether it was successful and complete or not)!
        return new LSFJobStatistics(jobId,
                startDate,
                endDate,
                cpuTime,
                maxMemory,
                maxSwap,
                maxProcesses,
                maxThreads,
                isSuccessful,
                isComplete);
    }

}