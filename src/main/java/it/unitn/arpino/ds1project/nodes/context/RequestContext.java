package it.unitn.arpino.ds1project.nodes.context;

import java.util.*;

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
