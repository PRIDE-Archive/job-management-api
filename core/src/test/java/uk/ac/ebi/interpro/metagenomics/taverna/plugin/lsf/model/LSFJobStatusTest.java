package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model;

import junit.framework.Assert;
import org.junit.Test;

/**
 * Represents a JUnit test class for {@link LSFJobStatus}.
 *
 * @author Maxim Scheremetjew, EMBL-EBI, InterPro
 * @version $Id: LSFJobStatusTest.java,v 1.1 2012/08/16 10:54:57 maxim Exp $
 * @since 1.0-SNAPSHOT
 */
public class LSFJobStatusTest {

    @Test
    public void testLSFJobStatus() {
        Assert.assertTrue("DONE".equals(LSFJobStatus.DONE.name()));
        Assert.assertTrue("EXIT".equals(LSFJobStatus.EXIT.name()));
        Assert.assertEquals("The job has terminated with status of 0.", LSFJobStatus.DONE.getDescription());
    }
}