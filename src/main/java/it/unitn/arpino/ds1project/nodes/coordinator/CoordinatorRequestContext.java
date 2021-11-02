package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import it.unitn.arpino.ds1project.nodes.context.RequestContext;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class CoordinatorRequestContext extends RequestContext {
    public enum TwoPhaseCommitFSM {
        INIT,
        WAIT,
        ABORT,
        COMMIT
    }

    /**
     * Duration (in seconds) within which all VoteResponses should be collected.
     */
    public static final int TIMEOUT_DURATION_S = 1;

    /**
     * The initiator of the transaction.
     */
    private final ActorRef client;

    /**
     * The servers that the coordinator has contacted in the context of the request.
     */
    private final Set<ActorRef> participants;

    /**
     * The servers that voted positively on the coordinator's request and are ready to commit.
     */
    private final Set<ActorRef> yesVoters;

    /**
     * Implements the timeout within which the Coordinator should receive all VoteResponses.
     */
    private Cancellable timeout;

    /**
     * The current state of the Two-phase commit protocol.
     */
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

    /**
     * @return The current state of the Two-phase commit (2PC) protocol.
     */
    public TwoPhaseCommitFSM getProtocolState() {
        return protocolState;
    }

    public void setProtocolState(TwoPhaseCommitFSM protocolState) {
        this.protocolState = protocolState;
    }

    public Set<ActorRef> getParticipants() {
        return participants;
    }

    public void addParticipant(ActorRef participant) {
        participants.add(participant);
    }

    public Set<ActorRef> getYesVoters() {
        return yesVoters;
    }

    public void addYesVoter(ActorRef yesVoter) {
        yesVoters.add(yesVoter);
    }

    boolean allVotedYes() {
        return yesVoters.size() == participants.size();
    }

    public void startTimer(Coordinator actor) {
        super.startTimer(actor, TIMEOUT_DURATION_S);
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
