package it.unitn.arpino.ds1project.simulation;

public class Simulation {
    public static final double DEFAULT_COORDINATOR_ON_VOTE_RESPONSE_CRASH_PROBABILITY = 0.0;
    public static final double DEFAULT_COORDINATOR_ON_VOTE_REQUEST_CRASH_PROBABILITY = 0.0;
    public static final double DEFAULT_COORDINATOR_ON_FINAL_DECISION_CRASH_PROBABILITY = 0.0;
    public static final long DEFAULT_COORDINATOR_RECOVERY_TIME_S = 3;
    public static final long DEFAULT_SERVER_RECOVERY_TIME_S = 3;

    public double coordinatorOnVoteResponseCrashProbability;
    public double coordinatorOnVoteRequestCrashProbability;
    public double coordinatorOnFinalDecisionCrashProbability;
    /**
     * Time (in seconds) after which a crashed coordinator recovers. Set to a negative number to keep it crashed forever.
     */
    public long coordinatorRecoveryTimeS;
    /**
     * Time (in seconds) after which a crashed server recovers. Set to a negative number to keep it crashed forever.
     */
    public long serverRecoveryTimeS;


    public Simulation() {
        this.coordinatorOnVoteResponseCrashProbability = DEFAULT_COORDINATOR_ON_VOTE_RESPONSE_CRASH_PROBABILITY;
        this.coordinatorOnVoteRequestCrashProbability = DEFAULT_COORDINATOR_ON_VOTE_REQUEST_CRASH_PROBABILITY;
        this.coordinatorOnFinalDecisionCrashProbability = DEFAULT_COORDINATOR_ON_FINAL_DECISION_CRASH_PROBABILITY;
        this.coordinatorRecoveryTimeS = DEFAULT_COORDINATOR_RECOVERY_TIME_S;
        this.serverRecoveryTimeS = DEFAULT_SERVER_RECOVERY_TIME_S;
    }
}
