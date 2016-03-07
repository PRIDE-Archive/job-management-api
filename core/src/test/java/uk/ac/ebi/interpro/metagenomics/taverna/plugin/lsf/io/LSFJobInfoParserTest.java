package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.io;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.management.LSFManager;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.LSFJobInformation;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.LSFJobStatus;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Represents a JUnit test for class {@link LSFJobInfoParser}.
 *
 * @author Maxim Scheremetjew, EMBL-EBI, InterPro
 * @version $Id: LSFJobInfoParserTest.java,v 1.5 2012/09/18 12:11:18 craigm Exp $
 * @since 1.0-SNAPSHOT
 */
public class LSFJobInfoParserTest {

    private InputStream is;

    @Test
    public void testParse() throws FileNotFoundException {
        LSFJobInfoParser parser = LSFManager.getLSFJobInfoParser();
        is = LSFJobInfoParserTest.class.getResourceAsStream("/monitorOutput.txt");
        Assert.assertNotNull("Input stream is unexpectedly NULL!", is);
        Map<String, Set<LSFJobInformation>> jobInfos = parser.parse(is);
        Assert.assertNotNull("List of job infos is unexpectedly NULL!", jobInfos);
        Assert.assertEquals("List of job infos not of expected size!", jobInfos.size(), 3);

        Date expectedDate = null;
        SimpleDateFormat df = new SimpleDateFormat("MMM dd HH:mm");
        try {
            expectedDate = df.parse("Aug 1 00:30");
        } catch (Exception e) {
            e.printStackTrace();
        }

        //Get first job information instance and test attributes
        Set<LSFJobInformation> jobInformations= jobInfos.get("projTestJob1");
        Iterator<LSFJobInformation> iterator = jobInformations.iterator();
        LSFJobInformation jobInformation = iterator.next();
        Assert.assertEquals("1057727",jobInformation.getJobId());
        Assert.assertEquals("ebi-046",jobInformation.getExecHost());
        Assert.assertEquals("iproscan",jobInformation.getFromHost());
        Assert.assertEquals("projTestJob1",jobInformation.getJobName());
        Assert.assertEquals("production",jobInformation.getQueueName());
        Assert.assertEquals(LSFJobStatus.DONE,jobInformation.getStatus());
        Assert.assertEquals(expectedDate,jobInformation.getSubmitTime());
        Assert.assertEquals("maxim",jobInformation.getUser());
    }

    @Test
    public void testFaultyHeader() throws FileNotFoundException {
        LSFJobInfoParser parser = LSFManager.getLSFJobInfoParser();
        is = LSFJobInfoParserTest.class.getResourceAsStream("/faultyHeaderOutput.txt");
        Assert.assertNotNull("Input stream is unexpectedly NULL!", is);
        try {
            Map<String, Set<LSFJobInformation>> jobInfos = parser.parse(is);
            Assert.fail("Faulty header not detected");
        } catch (ParseException e) {
            //Faulty header test succeeded
        }
    }

    @Test
    public void testFaultyJobInfo() throws FileNotFoundException {
        LSFJobInfoParser parser = LSFManager.getLSFJobInfoParser();
        is = LSFJobInfoParserTest.class.getResourceAsStream("/faultyJobInfoOutput.txt");
        Assert.assertNotNull("Input stream is unexpectedly NULL!", is);
        try {
            Map<String, Set<LSFJobInformation>> jobInfos = parser.parse(is);
            Assert.fail("Faulty job information line not detected");
        } catch (ParseException e) {
            //Faulty job information test succeeded

        }
    }

    @After
    public void closeInputStream() {
        if (is != null) {
            try {
                is.close();
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }
}