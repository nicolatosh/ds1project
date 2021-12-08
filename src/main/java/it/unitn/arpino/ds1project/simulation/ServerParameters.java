package it.unitn.arpino.ds1project.simulation;

public class ServerParameters extends Parameters {
    /**
     * Time (in seconds) after which a crashed server recovers. Set to a negative number to keep it crashed forever.
     */
    public long serverRecoveryTimeMs;
    public boolean serverCanRecover;
    public double serverOnVoteResponseCrashProbability;
    public double serverOnDecisionResponseCrashProbability;
    public double serverOnDecisionRequestCrashProbability;

    public ServerParameters() {
        serverCanRecover = Boolean.parseBoolean(cache.getProperty("serverCanRecover"));
        serverRecoveryTimeMs = Integer.parseInt(cache.getProperty("serverRecoveryTimeMs"));
        serverOnVoteResponseCrashProbability = Double.parseDouble(cache.getProperty("serverOnVoteResponseCrashProbability"));
        serverOnDecisionResponseCrashProbability = Double.parseDouble(cache.getProperty("serverOnDecisionResponseCrashProbability"));
        serverOnDecisionRequestCrashProbability = Double.parseDouble(cache.getProperty("serverOnDecisionRequestCrashProbability"));
    }
}
