package it.unitn.arpino.ds1project.nodes.context;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class RequestContextRepository<T extends RequestContext> {
    private final Set<T> contexts;

    public RequestContextRepository() {
        contexts = new HashSet<>();
    }

    public boolean existsContextWithId(UUID uuid) {
        return contexts.stream()
                .map(contexts -> contexts.uuid)
                .anyMatch(uuid::equals);
    }

    /**
     * Adds a {@link RequestContext} to the contexts.
     *
     * @param context RequestContext to add.
     */
    public void addRequestContext(T context) {
        contexts.add(context);
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    public T getRequestContextById(UUID uuid) {
        return contexts.stream()
                .filter(ctx -> ctx.uuid == uuid)
                .findAny()
                .get();
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
