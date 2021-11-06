package it.unitn.arpino.ds1project.nodes.context;

import java.util.UUID;

/**
 * Base class which is used to encapsulate everything related to a transaction.
 */
public abstract class RequestContext {
    public final UUID uuid;

    /**
     * @param uuid Unique identifier for this context.
     */
    public RequestContext(UUID uuid) {
        this.uuid = uuid;
    }

    /**
     * @return Whether the transaction of this context has already been committed or aborted.
     */
    public abstract boolean isDecided();
}
