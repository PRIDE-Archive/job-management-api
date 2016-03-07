package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.util;

import org.junit.Ignore;
import org.junit.Test;
import uk.ac.ebi.interpro.metagenomics.jobManager.SystemCommand;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.management.LSFManager;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.LSFBsubCommand;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.LSFJob;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents not a real JUnit test but a real job submission test.
 *
 * @author Maxim Scheremetjew, EMBL-EBI, InterPro
 * @version $Id: LSFJobSubmissionTest.java,v 1.5 2013/11/28 17:00:40 matthew Exp $
 * @since 1.0-SNAPSHOT
 */
public class LSFJobSubmissionTest {

    @Test
    @Ignore
    public void testLSFJobSubmission() {
        List<String> command = new ArrayList<String>();
        command.add("/ebi/production/interpro/production/python/python2/bin/python");
        command.add("/homes/maxim/projects/test/mg_pipeline/steps/test.py");

        SystemCommand systemCommand = new SystemCommand(command);
        LSFBsubCommandBuilder bsubCommandBuilder = new LSFBsubCommandBuilder(systemCommand);
        LSFBsubCommand bsubCommand = bsubCommandBuilder.
                setJobName("jobName").
                setQueueName("production-rh6").
                setMaxProcessors(8).
                setMinProcessors(2).
                setStdOutputFile("test.out").
                setStdErrOutputFile("test.err").
                setMaxMemory(500).
                setResourceRequirement("rusage[mem=500]").build();

        LSFJob lsfJob = new LSFJob(bsubCommand);
        LSFJobSubmitter jobSubmitter = LSFManager.getLSFJobSubmitter();
        jobSubmitter.submitJob(lsfJob);
    }
}
