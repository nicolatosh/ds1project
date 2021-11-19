package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import it.unitn.arpino.ds1project.messages.coordinator.ReadMsg;
import it.unitn.arpino.ds1project.messages.coordinator.VoteResponse;
import it.unitn.arpino.ds1project.messages.coordinator.VoteResponseTimeout;
import it.unitn.arpino.ds1project.messages.coordinator.WriteMsg;
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
        NONE,
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
     * Duration (in seconds) within which all the participants' {@link VoteResponse}s should be collected.
     */
    public static final int VOTE_RESPONSE_TIMEOUT_S = 1;

    /**
     * The initiator of the transaction.
     */
    private final ActorRef client;

    private final Collection<ActorRef> participants;

    private final Collection<ActorRef> yesVoters;

    private final List<LogState> localLog;

    private TwoPhaseCommitFSM protocolState;

    private Cancellable voteResponseTimer;

    public CoordinatorRequestContext(ActorRef client) {
        this.client = client;

        participants = new HashSet<>();
        yesVoters = new HashSet<>();
        localLog = new ArrayList<>();
    }

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
     * @return The participants involved in this transaction. A participant is a {@link Server} to which
     * the {@link Coordinator} has sent a {@link ReadRequest} or {@link WriteRequest}s to fulfill the client's
     * {@link ReadMsg} or {@link WriteMsg}.
     */
    public Collection<ActorRef> getParticipants() {
        return participants;
    }

    /**
     * Adds a participant to the list of participants of this transaction, if not already present. A participant is a
     * {@link Server} to which the {@link Coordinator} has sent a {@link ReadRequest} or {@link WriteRequest}s to
     * fulfill the client's {@link ReadMsg} or {@link WriteMsg}.
     */
    public void addParticipant(ActorRef participant) {
        if (!participants.contains(participant)) {
            participants.add(participant);
        }
    }

    /**
     * Adds a participant to the list of participants which, upon the {@link Coordinator}'s {@link VoteRequest},
     * cast a positive {@link VoteResponse}.
     */
    public void addYesVoter(ActorRef yesVoter) {
        yesVoters.add(yesVoter);
    }

    /**
     * @return Whether all participants have cast a positive {@link VoteResponse}. If true, the {@link Coordinator}
     * can request to commit the transaction, sending them the {@link FinalDecision}.
     */
    boolean allVotedYes() {
        return yesVoters.size() == participants.size();
    }

    public void log(LogState state) {
        localLog.add(state);
    }

    public LogState loggedState() {
        return localLog.get(localLog.size() - 1);
    }

    /**
     * Starts a countdown timer, within which the {@link Coordinator} should collect all participants'
     * {@link VoteResponse}s. If the responses do not arrive in time, the Coordinator assumes one participant has
     * crashed.
     */
    public void startVoteResponseTimer(Coordinator coordinator) {
        voteResponseTimer = coordinator.getContext().system().scheduler().scheduleOnce(
                Duration.ofSeconds(VOTE_RESPONSE_TIMEOUT_S), // delay
                coordinator.getSelf(), // receiver
                new VoteResponseTimeout(uuid), // message
                coordinator.getContext().dispatcher(), // executor
                coordinator.getSelf()); // sender
    }

    public void cancelVoteResponseTimeout() {
        voteResponseTimer.cancel();
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
