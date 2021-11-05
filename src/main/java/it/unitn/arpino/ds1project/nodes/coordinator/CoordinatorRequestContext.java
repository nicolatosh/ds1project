package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.messages.coordinator.ReadMsg;
import it.unitn.arpino.ds1project.messages.coordinator.VoteResponse;
import it.unitn.arpino.ds1project.messages.coordinator.WriteMsg;
import it.unitn.arpino.ds1project.messages.server.FinalDecision;
import it.unitn.arpino.ds1project.messages.server.ReadRequest;
import it.unitn.arpino.ds1project.messages.server.VoteRequest;
import it.unitn.arpino.ds1project.messages.server.WriteRequest;
import it.unitn.arpino.ds1project.nodes.context.RequestContext;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class CoordinatorRequestContext extends RequestContext {
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
    public static final int TIMEOUT_DURATION_S = 1;

    /**
     * The initiator of the transaction.
     */
    private final ActorRef client;

    private final Set<ActorRef> participants;

    private final Set<ActorRef> yesVoters;

    private TwoPhaseCommitFSM protocolState;

    public CoordinatorRequestContext(UUID uuid, ActorRef client) {
        super(uuid);
        this.client = client;

        protocolState = TwoPhaseCommitFSM.INIT;
        participants = new HashSet<>();
        yesVoters = new HashSet<>();
    }

    public ActorRef getClient() {
        return client;
    }

    @Override
    public boolean isDecided() {
        return protocolState == TwoPhaseCommitFSM.COMMIT || protocolState == TwoPhaseCommitFSM.ABORT;
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
    public Set<ActorRef> getParticipants() {
        return participants;
    }

    /**
     * Adds a participant to the list of participants of this transaction, if not already present. A participant is a
     * {@link Server} to which the {@link Coordinator} has sent a {@link ReadRequest} or {@link WriteRequest}s to
     * fulfill the client's {@link ReadMsg} or {@link WriteMsg}.
     */
    public void addParticipant(ActorRef participant) {
        participants.add(participant);
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

    /**
     * Starts a countdown timer, within which the {@link Coordinator} should receive the participants'
     * {@link VoteResponse}s. If the responses do not arrive in time, the Coordinator assumes one participant has
     * crashed.
     */
    public void startTimer(Coordinator coordinator) {
        super.startTimer(coordinator, TIMEOUT_DURATION_S);
    }

    @Override
    public String toString() {
        return "uuid: " + uuid +
                "\nclient: " + client.path().name() +
                "\ntwo-phase commit protocol state: " + protocolState +
                "\nparticipants: " + participants.stream().map(server -> server.path().name()).sorted().collect(Collectors.joining(", ")) +
                "\nyesVoters: " + yesVoters.stream().map(server -> server.path().name()).sorted().collect(Collectors.joining(", "));
    }
}
