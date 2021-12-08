package it.unitn.arpino.ds1project.simulation;

public class NetworkParameters extends Parameters {
    public boolean simulateNetworkDelays;
    public long minimumNetworkDelayMs;
    public long maximumNetworkDelayMs;

    public NetworkParameters() {
        simulateNetworkDelays = Boolean.parseBoolean(cache.getProperty("simulateNetworkDelays"));
        minimumNetworkDelayMs = Long.parseLong(cache.getProperty("minimumNetworkDelayMs"));
        maximumNetworkDelayMs = Long.parseLong(cache.getProperty("maximumNetworkDelayMs"));
    }
}
