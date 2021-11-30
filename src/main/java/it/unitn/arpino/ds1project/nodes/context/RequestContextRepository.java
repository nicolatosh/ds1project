package it.unitn.arpino.ds1project.nodes.context;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class RequestContextRepository<T extends RequestContext> {
    private final Set<T> contexts;

    public RequestContextRepository() {
        contexts = new HashSet<>();
    }

    /**
     * Adds a {@link RequestContext} to the contexts.
     *
     * @param context RequestContext to add.
     */
    public void addRequestContext(T context) {
        contexts.add(context);
    }

    public Optional<T> getRequestContextById(UUID uuid) {
        return contexts.stream()
                .filter(ctx -> ctx.uuid == uuid)
                .findAny();
    }

    public Collection<T> getAllRequestContexts() {
        return Set.copyOf(contexts);
    }

    public Collection<T> getAllRequestContexts(Predicate<T> predicate) {
        return contexts.stream()
                .filter(predicate)
                .collect(Collectors.toSet());
    }
}
