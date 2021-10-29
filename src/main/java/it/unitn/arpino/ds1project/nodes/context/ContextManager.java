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

    /**
     * @return The contexts of the transactions for which the coordinator has not yet taken the final decision.
     */
    public List<T> getActive() {
        return active;
    }

    public void setActive(T context) {
        context.setStatus(RequestContext.Status.ACTIVE);
        active.add(context);
    }

    /**
     * @return The contexts of the transactions for which the coordinator has taken the final decision.
     */
    public List<T> getCompleted() {
        return completed;
    }

    public void setCompleted(T context) {
        active.remove(context);
        context.setStatus(RequestContext.Status.COMPLETED);
        completed.add(context);
    }

    /**
     * @return The contexts of the transactions which have exceeded the maximum time for which they were
     * allowed to stay in an incomplete state.
     */
    public List<T> getExpired() {
        return expired;
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
