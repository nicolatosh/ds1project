package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.nodes.context.RequestContext;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

public class CoordinatorRequestContext extends RequestContext {
    public final ActorRef client;

    public enum TwoPhaseCommitFSM {
        INIT,
        WAIT,
        GLOBAL_ABORT,
        GLOBAL_COMMIT
    }

    /**
     * The servers that the coordinator has contacted in the context of the request.
     */
    public final Set<ActorRef> participants;

    public final Set<ActorRef> yesVoters;

    /**
     * The current state of the Two-phase commit protocol.
     */
    public TwoPhaseCommitFSM protocolState;

    public CoordinatorRequestContext(UUID uuid, ActorRef client) {
        super(uuid);
        this.client = client;

        protocolState = TwoPhaseCommitFSM.INIT;
        participants = new HashSet<>();
        yesVoters = new HashSet<>();
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
