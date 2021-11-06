package it.unitn.arpino.ds1project.nodes.context;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Base class which is used to encapsulate everything related to a transaction.
 */
public abstract class RequestContext {
    public final UUID uuid;

    protected final List<Enum<?>> localLog;

    /**
     * @param uuid Unique identifier for this context.
     */
    public RequestContext(UUID uuid) {
        this.uuid = uuid;
        localLog = new ArrayList<>();
    }

    /**
     * @return Whether the transaction of this context has already been committed or aborted.
     */
    public abstract boolean isDecided();

    public void log(Enum<?> state) {
        localLog.add(state);
    }

    public Optional<Enum<?>> loggedState() {
        if (localLog.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(localLog.get(localLog.size() - 1));
    }
}
