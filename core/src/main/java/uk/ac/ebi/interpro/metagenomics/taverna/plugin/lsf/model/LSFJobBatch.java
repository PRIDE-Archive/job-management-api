package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model;


import uk.ac.ebi.interpro.metagenomics.jobManager.GenericJobBatch;

import java.io.*;
import java.util.Set;

/**
 * Represents a batch of LSF jobs with the same project name.
 *
 * @author Maxim Scheremetjew, EMBL-EBI, InterPro
 * @version $Id: LSFJobBatch.java,v 1.6 2013/11/26 15:35:43 matthew Exp $
 * @since 1.0-SNAPSHOT
 */
public class LSFJobBatch extends GenericJobBatch implements Serializable {

    private String projectName;


    public LSFJobBatch(Set<? extends LSFJob> jobs, String projectName) {
        super(jobs);
        this.projectName = projectName;
    }

    public String getProjectName() {
        return projectName;
    }

    public int getBatchSize() {
        if (this.getJobs() != null) {
            return this.getJobs().size();
        }
        return 0;
    }

    public void serialize(String fileName) {
        try {
            FileOutputStream fileOut =  new FileOutputStream(fileName);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(this);
            out.close();
            fileOut.close();
        }   catch (IOException e) {
            throw new IllegalStateException("Could not serialize job batch to " + fileName, e );
        }
    }

    public static <T extends LSFJobBatch> T unSerialize(String fileName) {
        try {
            FileInputStream fileIn = new FileInputStream(fileName);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            T jobBatch = (T) in.readObject();
            in.close();
            fileIn.close();
            return jobBatch;

        }  catch (IOException ie) {
            throw new IllegalStateException("Could not unserialize job batch from " + fileName, ie );

        }  catch (ClassNotFoundException ce)  {
            throw new IllegalStateException("Could get LSFJobBatch from " + fileName, ce );
        }


    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LSFJobBatch)) return false;

        LSFJobBatch jobBatch = (LSFJobBatch) o;

        if (!projectName.equals(jobBatch.projectName)) return false;
        if (! (this.getBatchSize() == jobBatch.getBatchSize()) ) return false;
        Set<LSFJob> jobs1 = getJobs();
        Set<LSFJob> jobs2 = jobBatch.getJobs();
        for (LSFJob job: jobs1) {
            if (!jobs2.contains(job)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        return projectName.hashCode() + this.getBatchSize();
    }


    @Override
    public String toString() {
        return "LSFJobBatch{}";
    }
}
