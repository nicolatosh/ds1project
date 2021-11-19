package it.unitn.arpino.ds1project.nodes.context;

import java.util.Objects;
import java.util.UUID;

/**
 * Base class which is used to encapsulate everything related to a transaction.
 */
public abstract class RequestContext {
    public final UUID uuid;

    /**
     * Creates a new context with the specified identifier.
     *
     * @param uuid Unique identifier for this context.
     */
    public RequestContext(UUID uuid) {
        this.uuid = uuid;
    }

    /**
     * Creates a new context with a random identifier.
     */
    public RequestContext() {
        this(UUID.randomUUID());
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
