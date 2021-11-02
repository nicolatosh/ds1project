package it.unitn.arpino.ds1project.nodes;

import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.messages.Transactional;
import it.unitn.arpino.ds1project.nodes.context.RequestContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public abstract class DataStoreNode<T extends RequestContext> extends AbstractNode {
    public enum Status {
        ALIVE,
        CRASHED
    }

    private Status status;

    private final List<T> contexts;

    public DataStoreNode() {
        status = DataStoreNode.Status.ALIVE;
        contexts = new ArrayList<>();
    }

    Status getStatus() {
        return status;
    }

    protected void crash() {
        getContext().become(new ReceiveBuilder()
                .matchAny(msg -> {
                    // this suppresses Dead Letter warnings.
                }).build());
        status = DataStoreNode.Status.CRASHED;
    }

    protected void resume() {
        getContext().become(createReceive());
        status = DataStoreNode.Status.ALIVE;
    }

    public final Optional<T> getRequestContext(Transactional msg) {
        return contexts.stream()
                .filter(ctx -> ctx.uuid == msg.uuid())
                .findFirst();
    }

    public final void addContext(T ctx) {
        contexts.add(ctx);
    }

    /**
     * @return The contexts of the transactions for which the coordinator has taken the final decision.
     */
    public final List<T> getDecided() {
        return contexts.stream()
                .filter(RequestContext::isDecided)
                .collect(Collectors.toList());
    }

    /**
     * @return The contexts of the transactions for which the coordinator has not yet taken the final decision.
     */
    public final List<T> getActive() {
        return contexts.stream()
                .filter(ctx -> !ctx.isDecided())
                .collect(Collectors.toList());
    }
}
