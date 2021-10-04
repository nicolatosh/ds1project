package it.unitn.arpino.ds1project.nodes.context;

import it.unitn.arpino.ds1project.messages.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ContextManager<T extends RequestContext> {
    protected final List<T> contexts;

    public ContextManager() {
        this.contexts = new ArrayList<>();
    }

    public void save(T context) {
        contexts.add(context);
    }

    public void remove(T context) {
        contexts.remove(context);
    }

    public Optional<T> contextOf(Transactional msg) {
        return contexts.stream()
                .filter(ctx -> ctx.uuid() == msg.uuid())
                .findFirst();
    }
}
