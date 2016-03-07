package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.util;

import uk.ac.ebi.interpro.metagenomics.jobManager.SpeculativeCommand;
import uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model.*;

import java.util.*;

/**
 * Represents a factory for safe {@link LSFJobBatch} creation.
 *
 * @author Maxim Scheremetjew, EMBL-EBI, InterPro
 * @version $Id: LSFJobBatchFactory.java,v 1.7 2013/11/26 15:35:43 matthew Exp $
 * @since 1.0-SNAPSHOT
 */
public class LSFJobBatchFactory {

    private static final LSFJobBatchFactory instance = new LSFJobBatchFactory();

    private LSFJobBatchFactory() {
    }

    public static LSFJobBatchFactory getInstance() {
        return instance;
    }


    public LSFJobBatch createLSFBatch(final Set<LSFBatchItem> lsfBatchItems) {
        Set<LSFJob> result = new HashSet<LSFJob>();
        String projectName = getProjectName() ;


        int counter = 0;
        for (LSFBatchItem batchItem : lsfBatchItems) {

            counter++;
            //Create bsub command
            Map<LSFBsubOption, Object> options = batchItem.getOptions();
            options.put(LSFBsubOption.PROJECT_NAME, projectName);
            String jobName = projectName + counter;
            options.put(LSFBsubOption.JOB_NAME, jobName);
            LSFBsubCommand bsubCommand = new LSFBsubCommand(batchItem.getSystemCommand(), options);
            //Create a LSF job
            LSFJob batchJob = new LSFJob(bsubCommand);
            //Add job to the result collection
            result.add(batchJob);
        }
        return new LSFJobBatch(result, projectName);
    }


    public LSFSpeculativeJobBatch createSpeculativeLSFBatch(final Set<LSFSpeculativeBatchItem> lsfBatchItems, double speculationStart) {
        Set<LSFSpeculativeJob> newSet = new HashSet<LSFSpeculativeJob>();
        String projectName = getProjectName();

        int counter = 0;
        for (LSFSpeculativeBatchItem batchItem:lsfBatchItems) {
            counter++;
            //Create bsub command
            Map<LSFBsubOption, Object> options = batchItem.getOptions();
            options.put(LSFBsubOption.PROJECT_NAME, projectName);
            String jobName = projectName + counter;
            options.put(LSFBsubOption.JOB_NAME, jobName);
            LSFSpeculativeBsubCommand bsubCommand = new LSFSpeculativeBsubCommand((SpeculativeCommand)batchItem.getSystemCommand(), options);
            //Create a LSF job
            LSFSpeculativeJob batchJob = new LSFSpeculativeJob(bsubCommand);
            //Add job to the result collection
            newSet.add(batchJob);
        }
        return new LSFSpeculativeJobBatch(newSet, projectName, speculationStart);
    }

    public LSFSpeculativeJobBatch createSpeculativeLSFBatch(final Set<LSFSpeculativeBatchItem> lsfBatchItems) {
        return createSpeculativeLSFBatch(lsfBatchItems, LSFSpeculativeJobBatch.DEFAULT_SPECULATION_START);
    }



    private String getUniqueIdentifier() {
        return UUID.randomUUID().toString();
    }

    private String getProjectName() {
        String uniqueId = getUniqueIdentifier();
        return "proj" + uniqueId;
    }
}
