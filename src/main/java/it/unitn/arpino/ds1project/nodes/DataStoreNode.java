package it.unitn.arpino.ds1project.nodes;

import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.messages.Transactional;
import it.unitn.arpino.ds1project.messages.server.FinalDecision;
import it.unitn.arpino.ds1project.nodes.context.RequestContext;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Base class for the nodes of the distributed Data Store ({@link Coordinator}s and {@link Server}s).
 */
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

    protected final Status getStatus() {
        return status;
    }

    /**
     * Simulates a crash of the {@link DataStoreNode}. A crashed node will not handle the remaining messages in the
     * message queue and newly received ones.
     */
    protected void crash() {
        getContext().become(new ReceiveBuilder()
                .matchAny(msg -> {
                    // this suppresses Dead Letter warnings.
                }).build());
        status = DataStoreNode.Status.CRASHED;
    }

    /**
     * Resumes the {@link DataStoreNode} from a crash. A resumed note starts handling new messages.
     */
    protected void resume() {
        getContext().become(createReceive());
        status = DataStoreNode.Status.ALIVE;
    }

    /**
     * @return The {@link RequestContext} related to the {@link Transactional} message, if present.
     */
    public final Optional<T> getRequestContext(Transactional msg) {
        return contexts.stream()
                .filter(ctx -> ctx.uuid == msg.uuid())
                .findFirst();
    }

    public final void addContext(T ctx) {
        contexts.add(ctx);
    }

    /**
     * @return The {@link RequestContext}s for which the {@link FinalDecision} of the {@link Coordinator} is known.
     */
    public final List<T> getDecided() {
        return contexts.stream()
                .filter(RequestContext::isDecided)
                .collect(Collectors.toList());
    }

    /**
     * @return The {@link RequestContext}s for which the {@link FinalDecision} of the {@link Coordinator} is not known.
     */
    public final List<T> getActive() {
        return contexts.stream()
                .filter(ctx -> !ctx.isDecided())
                .collect(Collectors.toList());
    }
}
