package it.unitn.arpino.ds1project.simulation;

public class ServerParameters extends Parameters {
    /**
     * Time (in seconds) after which a crashed server recovers. Set to a negative number to keep it crashed forever.
     */
    public long serverRecoveryTimeMs;
    public boolean serverCanRecover;
    public double serverOnVoteResponseSuccessProbability;
    public double serverOnDecisionResponseSuccessProbability;
    public double serverOnDecisionRequestSuccessProbability;

    public ServerParameters() {
        serverCanRecover = Boolean.parseBoolean(cache.getProperty("serverCanRecover"));
        serverRecoveryTimeMs = Integer.parseInt(cache.getProperty("serverRecoveryTimeMs"));
        serverOnVoteResponseSuccessProbability = Double.parseDouble(cache.getProperty("serverOnVoteResponseSuccessProbability"));
        serverOnDecisionResponseSuccessProbability = Double.parseDouble(cache.getProperty("serverOnDecisionResponseSuccessProbability"));
        serverOnDecisionRequestSuccessProbability = Double.parseDouble(cache.getProperty("serverOnDecisionRequestSuccessProbability"));
    }
}
