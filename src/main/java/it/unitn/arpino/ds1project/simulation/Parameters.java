package it.unitn.arpino.ds1project.simulation;

import it.unitn.arpino.ds1project.nodes.DataStoreNode;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Parameters {
    private static final Properties cache;

    static {
        cache = new Properties();

        try (InputStream file = DataStoreNode.class.getResourceAsStream("/simulation.properties")) {
            if (file == null) {
                throw new FileNotFoundException("simulation.properties not found");
            }
            cache.load(file);

        } catch (IOException ignored) {
        }
    }

    public boolean clientLoop;
    public int clientMinTxnLength;
    public int clientMaxTxnLength;

    /**
     * Time (in seconds) after which a crashed coordinator recovers. Set to a negative number to keep it crashed forever.
     */
    public long coordinatorRecoveryTimeMs;
    public boolean coordinatorCanRecover;
    public double coordinatorOnVoteRequestCrashProbability;
    public double coordinatorOnFinalDecisionCrashProbability;

    /**
     * Time (in seconds) after which a crashed server recovers. Set to a negative number to keep it crashed forever.
     */
    public long serverRecoveryTimeMs;
    public boolean serverCanRecover;
    public double serverOnVoteResponseCrashProbability;
    public double serverOnDecisionResponseCrashProbability;
    public double serverOnDecisionRequestCrashProbability;

    public boolean simulateNetworkDelays;
    public long minimumNetworkDelayMs;
    public long maximumNetworkDelayMs;


    public Parameters() {
        clientLoop = Boolean.parseBoolean(cache.getProperty("clientLoop"));
        clientMinTxnLength = Integer.parseInt(cache.getProperty("clientMinTxnLength"));
        clientMaxTxnLength = Integer.parseInt(cache.getProperty("clientMaxTxnLength"));

        coordinatorCanRecover = Boolean.parseBoolean(cache.getProperty("coordinatorCanRecover"));
        coordinatorRecoveryTimeMs = Integer.parseInt(cache.getProperty("coordinatorRecoveryTimeMs"));
        coordinatorOnVoteRequestCrashProbability = Double.parseDouble(cache.getProperty("coordinatorOnVoteRequestCrashProbability"));
        coordinatorOnFinalDecisionCrashProbability = Double.parseDouble(cache.getProperty("coordinatorOnFinalDecisionCrashProbability"));

        serverCanRecover = Boolean.parseBoolean(cache.getProperty("serverCanRecover"));
        serverRecoveryTimeMs = Integer.parseInt(cache.getProperty("serverRecoveryTimeMs"));
        serverOnVoteResponseCrashProbability = Double.parseDouble(cache.getProperty("serverOnVoteResponseCrashProbability"));
        serverOnDecisionResponseCrashProbability = Double.parseDouble(cache.getProperty("serverOnDecisionResponseCrashProbability"));
        serverOnDecisionRequestCrashProbability = Double.parseDouble(cache.getProperty("serverOnDecisionRequestCrashProbability"));

        simulateNetworkDelays = Boolean.parseBoolean(cache.getProperty("simulateNetworkDelays"));
        minimumNetworkDelayMs = Long.parseLong(cache.getProperty("minimumNetworkDelayMs"));
        maximumNetworkDelayMs = Long.parseLong(cache.getProperty("maximumNetworkDelayMs"));
    }
}
