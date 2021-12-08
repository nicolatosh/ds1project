package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.messages.coordinator.Done;
import it.unitn.arpino.ds1project.messages.coordinator.ReadMsg;
import it.unitn.arpino.ds1project.messages.coordinator.VoteResponse;
import it.unitn.arpino.ds1project.messages.coordinator.WriteMsg;
import it.unitn.arpino.ds1project.messages.server.FinalDecision;
import it.unitn.arpino.ds1project.messages.server.ReadRequest;
import it.unitn.arpino.ds1project.messages.server.VoteRequest;
import it.unitn.arpino.ds1project.messages.server.WriteRequest;
import it.unitn.arpino.ds1project.nodes.context.RequestContext;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.*;
import java.util.stream.Collectors;

public class CoordinatorRequestContext extends RequestContext {
    public enum LogState {
        CONVERSATIONAL,
        START_2PC,
        GLOBAL_COMMIT,
        GLOBAL_ABORT
    }

    /**
     * Duration (in seconds) within which the {@link Coordinator} should receive a client's request after the last one it previously received.
     */
    public static final int CONVERSATIONAL_TIMEOUT = 4;

    /**
     * Duration (in seconds) within which the {@link Coordinator} should collect all the participants' {@link VoteResponse}s.
     */
    public static final int VOTE_RESPONSE_TIMEOUT_S = 4;

    /**
     * Duration (in seconds) which the {@link Coordinator}, cyclically, should solicit the participants to send a {@link Done} message.
     */
    public static final int DONE_TIMEOUT_S = 4;

    private final Collection<ActorRef> participants;

    private final Collection<ActorRef> yesVoters;

    private final Collection<ActorRef> doneParticipants;

    private final List<LogState> localLog;

    private boolean completed;

    public CoordinatorRequestContext(UUID uuid, ActorRef client) {
        super(uuid, client);

        participants = new HashSet<>();
        yesVoters = new HashSet<>();
        doneParticipants = new HashSet<>();
        localLog = new ArrayList<>();
    }

    @Override
    public boolean isDecided() {
        return loggedState() == LogState.GLOBAL_COMMIT || loggedState() == LogState.GLOBAL_ABORT;
    }

    /**
     * @return The participants involved in this transaction.
     * A participant is a {@link Server} to which the {@link Coordinator} has sent a {@link ReadRequest} or {@link WriteRequest}
     * to fulfill a client's {@link ReadMsg} or {@link WriteMsg}.
     */
    public Collection<ActorRef> getParticipants() {
        return new HashSet<>(participants);
    }

    /**
     * Adds a participant to the list of participants of this transaction, if not already present.
     * A participant is a {@link Server} to which the {@link Coordinator} has sent a {@link ReadRequest} or {@link WriteRequest}
     * to fulfill the client's {@link ReadMsg} or {@link WriteMsg}.
     */
    public void addParticipant(ActorRef participant) {
        if (!participants.contains(participant)) {
            participants.add(participant);
        }
    }

    /**
     * Removes a participant from the list of participants of this transaction, if present..
     */
    public void removeParticipant(ActorRef participant) {
        if (participants.contains(participant)) {
            participants.remove(participant);
        }
    }

    /**
     * Adds a participant to the list of those which have cast a positive {@link VoteResponse}
     * upon receiving the {@link Coordinator}'s {@link VoteRequest},
     */
    public void addYesVoter(ActorRef yesVoter) {
        yesVoters.add(yesVoter);
    }

    /**
     * @return Whether all participants have cast a positive {@link VoteResponse}.
     * If true, the {@link Coordinator} can commit the transaction, sending them the {@link FinalDecision}.
     */
    boolean allVotedYes() {
        return yesVoters.size() == participants.size();
    }

    public void addDoneParticipant(ActorRef participant) {
        if (!doneParticipants.contains(participant)) {
            doneParticipants.add(participant);
        }
    }

    public boolean allParticipantsDone() {
        return doneParticipants.size() == participants.size();
    }

    Collection<ActorRef> getRemainingDoneParticipants() {
        Collection<ActorRef> missing = new HashSet<>(participants);
        missing.removeAll(doneParticipants);
        return missing;
    }

    /**
     * Writes something to the local log.
     *
     * @param state The state to log.
     */
    public void log(LogState state) {
        localLog.add(state);
    }

    /**
     * @return The logged state in the local log.
     */
    public LogState loggedState() {
        return localLog.get(localLog.size() - 1);
    }

    /**
     * @return Whether the client "should" know the result of the transaction (unless it crashed before receiving the result).
     * This variable is used by the coordinator to determine whether it should retransmit the final decision to the participants
     * or the transaction result to the client when resuming from a crash.
     */
    public boolean isCompleted() {
        return completed;
    }

    /**
     * Sets the context as completed, meaning that the client "should" know the result of the transaction
     * (unless it crashed before receiving the result).
     */
    public void setCompleted() {
        this.completed = true;
    }

    @Override
    public String toString() {
        return "uuid: " + uuid +
                "\nclient: " + subject.path().name() +
                "\nlogged state: " + loggedState() +
                "\nparticipants: " + participants.stream().map(server -> server.path().name()).sorted().collect(Collectors.joining(", ")) +
                "\nyes voters: " + yesVoters.stream().map(server -> server.path().name()).sorted().collect(Collectors.joining(", "));
    }
}
