package it.unitn.arpino.ds1project.nodes.context;

import akka.actor.ActorRef;

import java.util.Objects;
import java.util.UUID;

/**
 * Base class which is used to encapsulate everything related to a transaction.
 */
public abstract class RequestContext {
    public final UUID uuid;
    public final ActorRef subject;

    /**
     * Creates a new context with the specified identifier.
     *
     * @param uuid    Unique identifier for this context.
     * @param subject The subject of this context.
     */
    public RequestContext(UUID uuid, ActorRef subject) {
        this.uuid = uuid;
        this.subject = subject;
    }

    /**
     * @return Whether the outcome of the transaction has already been determined.
     */
    public abstract boolean isDecided();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RequestContext)) return false;
        RequestContext that = (RequestContext) o;
        return uuid.equals(that.uuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid);
    }
}
