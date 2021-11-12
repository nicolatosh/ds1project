package it.unitn.arpino.ds1project.nodes;

import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.messages.server.FinalDecision;
import it.unitn.arpino.ds1project.nodes.context.RequestContext;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;
import it.unitn.arpino.ds1project.simulation.Simulation;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Base class for the nodes of the distributed Data Store ({@link Coordinator}s and {@link Server}s).
 */
public abstract class DataStoreNode<T extends RequestContext> extends AbstractNode {
    public enum Status {
        ALIVE,
        CRASHED
    }

    private Simulation parameters;

    private Status status;

    private final List<T> contexts;

    public DataStoreNode() {
        parameters = new Simulation();
        status = DataStoreNode.Status.ALIVE;
        contexts = new ArrayList<>();
    }

    public final Status getStatus() {
        return status;
    }

    public Simulation getParameters() {
        return parameters;
    }

    /**
     * Simulates a crash of the {@link DataStoreNode}. A crashed node will not handle the remaining messages in the
     * message queue and newly received ones.
     */
    protected void crash() {
        logger.info("Crashing...");
        getContext().become(new ReceiveBuilder()
                .matchAny(msg -> {
                    // this suppresses Dead Letter warnings.
                }).build());
        status = DataStoreNode.Status.CRASHED;
    }

    /**
     * Resumes the {@link DataStoreNode} from a crash. A resumed note starts handling new messages.
     * This method implements the recovery actions of the Two-phase commit (2PC) protocol.
     */
    protected void resume() {
        logger.info("Resuming...");
        getContext().become(createReceive());
        status = DataStoreNode.Status.ALIVE;
    }

    /**
     * @return The {@link RequestContext} related to the message, if present.
     */
    public final Optional<T> getRequestContext(UUID uuid) {
        return contexts.stream()
                .filter(ctx -> ctx.uuid.equals(uuid))
                .findFirst();
    }

    public final void addContext(T ctx) {
        if (!contexts.contains(ctx)) {
            contexts.add(ctx);
        }
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
