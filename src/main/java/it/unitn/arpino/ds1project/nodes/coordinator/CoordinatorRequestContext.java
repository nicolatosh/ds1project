package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import it.unitn.arpino.ds1project.messages.coordinator.*;
import it.unitn.arpino.ds1project.messages.server.FinalDecision;
import it.unitn.arpino.ds1project.messages.server.ReadRequest;
import it.unitn.arpino.ds1project.messages.server.VoteRequest;
import it.unitn.arpino.ds1project.messages.server.WriteRequest;
import it.unitn.arpino.ds1project.nodes.context.RequestContext;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class CoordinatorRequestContext extends RequestContext {
    public enum LogState {
        CONVERSATIONAL,
        START_2PC,
        GLOBAL_COMMIT,
        GLOBAL_ABORT
    }

    /**
     * State of the Two-phase commit (2PC) protocol of a transaction.
     */
    public enum TwoPhaseCommitFSM {
        INIT,
        WAIT,
        ABORT,
        COMMIT
    }

    /**
     * Duration (in seconds) within which the {@link Coordinator} should collect all the participants' {@link VoteResponse}s.
     */
    public static final int VOTE_RESPONSE_TIMEOUT_S = 1;

    /**
     * Duration (in seconds) within which the {@link Coordinator} should receive the client's {@link TxnEndMsg}.
     */
    public static final int TXN_END_TIMEOUT_S = 2;

    private final ActorRef client;

    private final Collection<ActorRef> participants;

    private final Collection<ActorRef> yesVoters;

    private final List<LogState> localLog;

    private TwoPhaseCommitFSM protocolState;

    private Cancellable voteResponseTimer;

    private Cancellable txnEndTimer;

    public CoordinatorRequestContext(ActorRef client) {
        this.client = client;

        participants = new HashSet<>();
        yesVoters = new HashSet<>();
        localLog = new ArrayList<>();
    }

    /**
     * The initiator of the transaction.
     */
    public ActorRef getClient() {
        return client;
    }

    @Override
    public boolean isDecided() {
        return loggedState() == LogState.GLOBAL_COMMIT || loggedState() == LogState.GLOBAL_ABORT;
    }

    /**
     * @return The current state of the Two-phase commit (2PC) protocol of the transaction.
     */
    public TwoPhaseCommitFSM getProtocolState() {
        return protocolState;
    }

    /**
     * Sets the current state of the Two-phase commit (2PC) protocol of the transaction.
     */
    public void setProtocolState(TwoPhaseCommitFSM protocolState) {
        this.protocolState = protocolState;
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
     * Starts a countdown timer, within which the {@link Coordinator} should collect all the participants' {@link VoteResponse}s.
     * If the responses do not arrive in time, the Coordinator assumes one participant has crashed, and sends to itself
     * a {@link VoteResponseTimeout}.
     */
    public void startVoteResponseTimer(Coordinator coordinator) {
        cancelVoteResponseTimeout();
        voteResponseTimer = coordinator.getContext().system().scheduler().scheduleOnce(
                Duration.ofSeconds(VOTE_RESPONSE_TIMEOUT_S), // delay
                coordinator.getSelf(), // receiver
                new VoteResponseTimeout(uuid), // message
                coordinator.getContext().dispatcher(), // executor
                coordinator.getSelf()); // sender
    }

    public void cancelVoteResponseTimeout() {
        if (voteResponseTimer != null) {
            voteResponseTimer.cancel();
        }
    }

    /**
     * Starts a countdown timer, within which the {@link Coordinator} should receive the client's {@link TxnEndMsg}.
     * If the message does not arrive in time, the Coordinator sends to itself a {@link TxnEndTimeout}.
     */
    public void startTxnEndTimer(Coordinator coordinator) {
        cancelTxnEndTimer();
        txnEndTimer = coordinator.getContext().system().scheduler().scheduleOnce(
                Duration.ofSeconds(TXN_END_TIMEOUT_S), // delay
                coordinator.getSelf(), // receiver
                new TxnEndTimeout(uuid), // message
                coordinator.getContext().dispatcher(), // executor
                coordinator.getSelf()); // sender
    }

    public void cancelTxnEndTimer() {
        if (txnEndTimer != null) {
            txnEndTimer.cancel();
        }
    }

    @Override
    public String toString() {
        return "uuid: " + uuid +
                "\nclient: " + client.path().name() +
                "\nlogged state: " + loggedState() +
                "\nprotocol state: " + protocolState +
                "\nparticipants: " + participants.stream().map(server -> server.path().name()).sorted().collect(Collectors.joining(", ")) +
                "\nyes voters: " + yesVoters.stream().map(server -> server.path().name()).sorted().collect(Collectors.joining(", "));
    }
}
