package it.unitn.arpino.ds1project.nodes;

import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.messages.JoinMessage;
import it.unitn.arpino.ds1project.messages.ResumeMessage;
import it.unitn.arpino.ds1project.messages.StartMessage;
import it.unitn.arpino.ds1project.messages.TxnMessage;
import it.unitn.arpino.ds1project.messages.coordinator.TxnBeginMsg;
import it.unitn.arpino.ds1project.messages.server.FinalDecision;
import it.unitn.arpino.ds1project.nodes.context.RequestContext;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;
import it.unitn.arpino.ds1project.simulation.Simulation;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

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

    private final Receive aliveReceive;
    private final Receive crashedReceive;

    private final Simulation parameters;

    private Status status;

    /**
     * {@link RequestContext}s that the node has finished or is yet processing.
     */
    private final List<T> contexts;

    public DataStoreNode() {
        parameters = new Simulation();
        status = DataStoreNode.Status.ALIVE;
        contexts = new ArrayList<>();

        aliveReceive = createAliveReceive();
        crashedReceive = createCrashedReceive();
    }

    protected abstract Receive createAliveReceive();

    private Receive createCrashedReceive() {
        return receiveBuilder()
                .match(ResumeMessage.class, msg -> resume())
                .matchAny(msg -> {
                    // this suppresses Dead Letter warnings.
                })
                .build();
    }

    public final Status getStatus() {
        return status;
    }

    public Simulation getParameters() {
        return parameters;
    }

    @Override
    public Receive createReceive() {
        return new ReceiveBuilder()
                .match(JoinMessage.class, this::onJoinMessage)
                .match(StartMessage.class, this::onStartMsg)
                .matchAny(msg -> {
                    logger.info("Stashed " + msg.getClass().getSimpleName() + " from " + getSender().path().name());
                    stash();
                })
                .build();
    }

    @Override
    public void aroundReceive(PartialFunction<Object, BoxedUnit> receive, Object obj) {
        if (obj instanceof TxnMessage) {
            TxnMessage msg = (TxnMessage) obj;

            switch (getStatus()) {
                case ALIVE: {
                    if (msg instanceof TxnBeginMsg) {
                        logger.info("Received " + msg.getClass().getSimpleName() + " from " + getSender().path().name());
                    } else {
                        logger.info("Received " + msg.getClass().getSimpleName() + " from " + getSender().path().name() + " with UUID " + msg.uuid);
                    }
                    break;
                }
                case CRASHED: {
                    if (msg instanceof TxnBeginMsg) {
                        logger.info("Dropped " + msg.getClass().getSimpleName() + " from " + getSender().path().name());
                    } else {
                        logger.info("Dropped " + msg.getClass().getSimpleName() + " from " + getSender().path().name() + " with UUID " + msg.uuid);
                    }
                    return;
                }
            }
        }

        super.aroundReceive(receive, obj);
    }

    protected abstract void onJoinMessage(JoinMessage msg);

    private void onStartMsg(StartMessage msg) {
        logger.info("Starting");
        unstashAll();
        getContext().become(aliveReceive);
    }

    /**
     * Simulates a crash of the node.
     * A crashed node will not handle the remaining messages in the message queue nor newly received ones
     * until resume() is called.
     */
    protected void crash() {
        logger.info("Crashing...");
        getContext().become(crashedReceive);
        status = DataStoreNode.Status.CRASHED;
    }

    /**
     * Resumes the node from a crash.
     * A resumed note starts handling new messages.
     * A node must override this method to implement the recovery actions of the Two-phase commit (2PC) protocol.
     */
    protected void resume() {
        logger.info("Resuming...");
        getContext().become(aliveReceive);
        status = DataStoreNode.Status.ALIVE;
    }

    /**
     * @param uuid Identifier of the {@link RequestContext} to obtain.
     * @return The {@link RequestContext} with the provided identifier, if present.
     */
    public final Optional<T> getRequestContext(UUID uuid) {
        return contexts.stream()
                .filter(ctx -> ctx.uuid.equals(uuid))
                .findFirst();
    }

    /**
     * Adds a {@link RequestContext} to the list of contexts.
     *
     * @param ctx RequestContext to add to the list.
     */
    public final void addContext(T ctx) {
        if (!contexts.contains(ctx)) {
            contexts.add(ctx);
        }
    }

    /**
     * Remove a {@link RequestContext} to the list of contexts.
     *
     * @param ctx RequestContext to remove from the list.
     */
    public final void removeContext(T ctx) {
        contexts.remove(ctx);
    }

    /**
     * @return The {@link RequestContext}s for which the {@link FinalDecision} is known.
     */
    public final List<T> getDecided() {
        return contexts.stream()
                .filter(RequestContext::isDecided)
                .collect(Collectors.toList());
    }

    /**
     * @return The {@link RequestContext}s for which the {@link FinalDecision} is not known.
     */
    public final List<T> getActive() {
        return contexts.stream()
                .filter(ctx -> !ctx.isDecided())
                .collect(Collectors.toList());
    }
}
