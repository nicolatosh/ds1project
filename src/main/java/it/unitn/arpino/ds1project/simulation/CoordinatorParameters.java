package it.unitn.arpino.ds1project.simulation;

public class CoordinatorParameters extends Parameters {
    /**
     * Time (in seconds) after which a crashed coordinator recovers. Set to a negative number to keep it crashed forever.
     */
    public long coordinatorRecoveryTimeMs;
    public boolean coordinatorCanRecover;
    public double coordinatorOnVoteRequestCrashProbability;
    public double coordinatorOnFinalDecisionCrashProbability;

    public CoordinatorParameters() {
        coordinatorCanRecover = Boolean.parseBoolean(cache.getProperty("coordinatorCanRecover"));
        coordinatorRecoveryTimeMs = Integer.parseInt(cache.getProperty("coordinatorRecoveryTimeMs"));
        coordinatorOnVoteRequestCrashProbability = Double.parseDouble(cache.getProperty("coordinatorOnVoteRequestCrashProbability"));
        coordinatorOnFinalDecisionCrashProbability = Double.parseDouble(cache.getProperty("coordinatorOnFinalDecisionCrashProbability"));
    }
}
