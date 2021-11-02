package it.unitn.arpino.ds1project.nodes.context;

import it.unitn.arpino.ds1project.messages.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ContextManager<T extends RequestContext> {
    private final List<T> contexts;


    public ContextManager() {
        contexts = new ArrayList<>();
    }

    public void add(T context) {
        contexts.add(context);
    }

    /**
     * @return The contexts of the transactions for which the coordinator has not yet taken the final decision.
     */
    public List<T> getActive() {
        return contexts.stream()
                .filter(ctx -> !ctx.isCompleted())
                .collect(Collectors.toList());
    }


    /**
     * @return The contexts of the transactions for which the coordinator has taken the final decision.
     */
    public List<T> getCompleted() {
        return contexts.stream()
                .filter(RequestContext::isCompleted)
                .collect(Collectors.toList());
    }

    public Optional<T> contextOf(Transactional msg) {
        return contexts.stream()
                .filter(ctx -> ctx.uuid == msg.uuid())
                .findFirst();
    }
}
