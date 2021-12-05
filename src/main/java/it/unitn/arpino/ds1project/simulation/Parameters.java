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

    /**
     * Time (in seconds) after which a crashed coordinator recovers. Set to a negative number to keep it crashed forever.
     */
    public long coordinatorRecoveryTimeS;
    public double coordinatorOnVoteRequestCrashProbability;
    public double coordinatorOnFinalDecisionCrashProbability;

    /**
     * Time (in seconds) after which a crashed server recovers. Set to a negative number to keep it crashed forever.
     */
    public long serverRecoveryTimeS;
    public double serverOnVoteResponseCrashProbability;
    public double serverOnDecisionResponseCrashProbability;
    public double serverOnDecisionRequestCrashProbability;

    public boolean simulateNetworkDelays;
    public long minimumNetworkDelayMs;
    public long maximumNetworkDelayMs;


    public Parameters() {
        coordinatorRecoveryTimeS = Integer.parseInt(cache.getProperty("coordinatorRecoveryTimeS"));
        coordinatorOnVoteRequestCrashProbability = Integer.parseInt(cache.getProperty("coordinatorOnVoteRequestCrashProbability"));
        coordinatorOnFinalDecisionCrashProbability = Integer.parseInt(cache.getProperty("coordinatorOnFinalDecisionCrashProbability"));

        serverRecoveryTimeS = Integer.parseInt(cache.getProperty("serverRecoveryTimeS"));
        serverOnVoteResponseCrashProbability = Integer.parseInt(cache.getProperty("serverOnVoteResponseCrashProbability"));
        serverOnDecisionResponseCrashProbability = Integer.parseInt(cache.getProperty("serverOnDecisionResponseCrashProbability"));
        serverOnDecisionRequestCrashProbability = Integer.parseInt(cache.getProperty("serverOnDecisionRequestCrashProbability"));

        simulateNetworkDelays = Boolean.parseBoolean(cache.getProperty("simulateNetworkDelays"));
        minimumNetworkDelayMs = Long.parseLong(cache.getProperty("minimumNetworkDelayMs"));
        maximumNetworkDelayMs = Long.parseLong(cache.getProperty("maximumNetworkDelayMs"));
    }
}
