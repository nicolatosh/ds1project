package it.unitn.arpino.ds1project.nodes;

import akka.actor.AbstractActorWithStash;
import it.unitn.arpino.ds1project.messages.ResumeMessage;
import it.unitn.arpino.ds1project.nodes.context.RequestContext;
import it.unitn.arpino.ds1project.nodes.context.RequestContextRepository;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * Base class for the nodes of the distributed Data Store ({@link Coordinator}s and {@link Server}s).
 */
public abstract class DataStoreNode<T extends RequestContext> extends AbstractActorWithStash {
    public enum Status {
        ALIVE,
        CRASHED
    }

    protected final Logger logger;

    private Status status;

    private final RequestContextRepository<T> repository;

    public DataStoreNode() {
        try (InputStream config = DataStoreNode.class.getResourceAsStream("/logging.properties")) {
            if (config != null) {
                LogManager.getLogManager().readConfiguration(config);
            }
        } catch (IOException ignored) {
        }

        logger = Logger.getLogger(getSelf().path().name());

        status = DataStoreNode.Status.ALIVE;
        repository = new RequestContextRepository<>();
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

    public RequestContextRepository<T> getRepository() {
        return repository;
    }

    /**
     * Simulates a crash of the node.
     * A crashed node will not handle the remaining messages in the message queue nor newly received ones
     * until resume() is called.
     */
    protected void crash() {
        logger.info("Crashing...");
        getContext().become(createCrashedReceive());
        status = DataStoreNode.Status.CRASHED;
    }

    /**
     * Resumes the node from a crash.
     * A resumed note starts handling new messages.
     * A node must override this method to implement the recovery actions of the Two-phase commit (2PC) protocol.
     */
    protected void resume() {
        logger.info("Resuming...");
        getContext().become(createAliveReceive());
        status = DataStoreNode.Status.ALIVE;
    }
}
