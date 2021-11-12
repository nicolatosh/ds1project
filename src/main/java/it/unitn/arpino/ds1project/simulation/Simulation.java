package it.unitn.arpino.ds1project.simulation;

public class Simulation {
    public static final double DEFAULT_COORDINATOR_ON_VOTE_RESPONSE_CRASH_PROBABILITY = 0.0;
    public static final double DEFAULT_COORDINATOR_ON_VOTE_REQUEST_CRASH_PROBABILITY = 0.0;
    public static final double DEFAULT_COORDINATOR_ON_FINAL_DECISION_CRASH_PROBABILITY = 0.0;

    public double coordinatorOnVoteResponseCrashProbability;
    public double coordinatorOnVoteRequestCrashProbability;
    public double coordinatorOnFinalDecisionCrashProbability;

    public Simulation() {
        this.coordinatorOnVoteResponseCrashProbability = DEFAULT_COORDINATOR_ON_VOTE_RESPONSE_CRASH_PROBABILITY;
        this.coordinatorOnVoteRequestCrashProbability = DEFAULT_COORDINATOR_ON_VOTE_REQUEST_CRASH_PROBABILITY;
        this.coordinatorOnFinalDecisionCrashProbability = DEFAULT_COORDINATOR_ON_FINAL_DECISION_CRASH_PROBABILITY;

    }
}
