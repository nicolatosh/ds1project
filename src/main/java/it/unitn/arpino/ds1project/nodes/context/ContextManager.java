package it.unitn.arpino.ds1project.nodes.context;

import it.unitn.arpino.ds1project.messages.Transactional;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class ContextManager<T extends RequestContext> {
    protected final List<T> active;
    protected final List<T> completed;
    protected final List<T> expired;


    public ContextManager() {
        this.active = new ArrayList<>();
        this.completed = new ArrayList<>();
        this.expired = new ArrayList<>();
    }

    public void setActive(T context) {
        context.setStatus(RequestContext.Status.ACTIVE);
        active.add(context);
    }

    public void setCompleted(T context) {
        active.remove(context);
        context.setStatus(RequestContext.Status.COMPLETED);
        completed.add(context);
    }

    public void setExpired(T context) {
        active.remove(context);
        context.setStatus(RequestContext.Status.EXPIRED);
        expired.add(context);
    }

    public Optional<T> contextOf(Transactional msg) {
        return Stream.of(active, completed, expired)
                .flatMap(Collection::stream)
                .filter(ctx -> ctx.uuid == msg.uuid())
                .findFirst();
    }
}
