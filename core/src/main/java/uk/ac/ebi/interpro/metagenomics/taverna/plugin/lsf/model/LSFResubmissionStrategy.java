package uk.ac.ebi.interpro.metagenomics.taverna.plugin.lsf.model;

/**
 * Created with IntelliJ IDEA.
 * User: craigm
 * Date: 21/08/12
 * Time: 10:37
 * To change this template use File | Settings | File Templates.
 */
public class LSFResubmissionStrategy {

    private static final int DEFAULT_MAX_FAILURES = 1;
    private static final int NO_MEMORY_ALTERATION = 0;
    private final int maxFailures;
    private final int resubmissionMemory;

    public LSFResubmissionStrategy() {
        this(DEFAULT_MAX_FAILURES, NO_MEMORY_ALTERATION);
    }

    public LSFResubmissionStrategy(int maxFailures) {
        this(maxFailures, NO_MEMORY_ALTERATION);
    }

    public LSFResubmissionStrategy(int maxFailures, int resubmissionMemory) {
        this.resubmissionMemory = resubmissionMemory;
        this.maxFailures = maxFailures;
    }

    public int getMaxFailures() {
        return maxFailures;
    }

    public int getResubmissionMemory() {
        return resubmissionMemory;
    }

    public boolean alterMemoryForResubmission() {
        return !(this.resubmissionMemory == NO_MEMORY_ALTERATION);
    }

}
