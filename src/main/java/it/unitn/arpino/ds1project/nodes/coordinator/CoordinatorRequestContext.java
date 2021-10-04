package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.nodes.context.RequestContext;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class CoordinatorRequestContext implements RequestContext {
    private final UUID uuid;

    public final ActorRef client;

    public enum STATE {
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
    public STATE state;

    public CoordinatorRequestContext(ActorRef client) {
        uuid = UUID.randomUUID();
        this.client = client;

        state = STATE.INIT;
        participants = new HashSet<>();
        yesVoters = new HashSet<>();
    }

    @Override
    public UUID uuid() {
        return uuid;
    }
}