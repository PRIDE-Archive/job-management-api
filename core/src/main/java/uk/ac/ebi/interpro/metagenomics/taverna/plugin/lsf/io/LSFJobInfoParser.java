package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.io;

import org.apache.log4j.Logger;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.LSFJobInformation;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * Created with IntelliJ IDEA.
 * User: craigm
 * Date: 14/08/12
 * Time: 11:16
 * To change this template use File | Settings | File Templates.
 */
public class LSFJobInfoParser {

    private static final String HEADER_LINE = "JOBID   USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME";
    private static final int JOB_INFORMATION_LINE_LENGTH = 10;
    private static final int JOBID_POS = 0;
    private static final int USER_POS = 1;
    private static final int STATUS_POS = 2;
    private static final int QUEUE_POS = 3;
    private static final int FROM_HOST_POS = 4;
    private static final int EXEC_HOST_POS = 5;
    private static final int JOB_NAME_POS = 6;
    private static final int SUBMIT_TIME_MONTH_POS = 7;
    private static final int SUBMIT_TIME_DAY_POS = 8;
    private static final int SUBMIT_TIME_TIME_POS = 9;

    private static final Logger LOGGER = Logger.getLogger(LSFJobInfoParser.class.getName());

    private static final LSFJobInfoParser instance = new LSFJobInfoParser();

    private LSFJobInfoParser() {
    }

    public static LSFJobInfoParser getInstance() {
        return instance;
    }


    public Map<String, Set<LSFJobInformation>> parse(InputStream is) {
        Map<String, Set<LSFJobInformation>> jobInformations = new HashMap<String, Set<LSFJobInformation>>();
        BufferedInputStream buffer = new BufferedInputStream(is);
        BufferedReader textout = new BufferedReader(new InputStreamReader(buffer));
        String line;
        try {
            while ((line = textout.readLine()) != null) {
                if (line.startsWith("JOBID")) {
                    if (!line.equals(HEADER_LINE)) {
                        throw new ParseException("The header line format is not as expected. Expected format:" + HEADER_LINE, line);
                    }
                } else {
                    String[] splitLine = line.split("\\s+");
                    if (!(splitLine.length == JOB_INFORMATION_LINE_LENGTH)) {
                        throw new ParseException("The job information line does not have the expected number of fields", line);
                    }
                    String jobId = splitLine[JOBID_POS];
                    String user = splitLine[USER_POS];
                    String status = splitLine[STATUS_POS];
                    String queueName = splitLine[QUEUE_POS];
                    String fromHost = splitLine[FROM_HOST_POS];
                    String execHost = splitLine[EXEC_HOST_POS];
                    String jobName = splitLine[JOB_NAME_POS];
                    Date date = getDate(splitLine);
                    LSFJobInformation jobInformation = new LSFJobInformation(jobId, user, status, queueName,
                            fromHost, execHost, jobName, date);
                    if (jobInformations.containsKey(jobName)) {
                       Set jobInformationSet = jobInformations.get(jobName);
                        jobInformationSet.add(jobInformation);

                    }  else {
                        Set jobInformationSet = new HashSet();
                        jobInformationSet.add(jobInformation);
                        jobInformations.put(jobName, jobInformationSet);
                    }
                }

            }

        } catch (IOException e) {
            e.printStackTrace();
        }  finally {
            try {
                textout.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                is.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return jobInformations;


    }

    private Date getDate(String[] splitLine) {

        String delimiter = " ";
        StringBuilder sb = new StringBuilder();
        sb.append(splitLine[SUBMIT_TIME_MONTH_POS]).append(delimiter)
                .append(splitLine[SUBMIT_TIME_DAY_POS]).append(delimiter)
                .append(splitLine[SUBMIT_TIME_TIME_POS]);
        SimpleDateFormat df = new SimpleDateFormat("MMM dd HH:mm");
        try {
            return df.parse(sb.toString());

        } catch (java.text.ParseException e) {
            throw new ParseException("Could not convert the SUBMIT_TIME field into a date. Expected format = MMM dd EEE HH:mm.", sb.toString());

        }
    }
}
