package it.unitn.arpino.ds1project.nodes.context;

import akka.actor.Cancellable;
import it.unitn.arpino.ds1project.messages.TimeoutExpired;
import it.unitn.arpino.ds1project.messages.TimeoutExpired.TIMEOUT_TYPE;
import it.unitn.arpino.ds1project.nodes.DataStoreNode;

import java.time.Duration;
import java.util.UUID;

/**
 * Base class which is used to encapsulate everything related to a transaction.
 */
public abstract class RequestContext {
    public final UUID uuid;

    private Cancellable finalDecisionTimeout;
    private Cancellable voteRequestTimeout;
    private Cancellable voteResponseTimeout;

    /**
     * @param uuid Unique identifier for this context.
     */
    public RequestContext(UUID uuid) {
        this.uuid = uuid;
    }

    /**
     * Starts a countdown timer. If the timer is not canceled before expiring, sends a {@link TimeoutExpired} message
     * to the {@link DataStoreNode} which started the timer.
     *
     * @param node            The node which starts the timer.
     * @param timeoutDuration Duration in seconds of the timer.
     */
    public void startFinalDecisionTimeout(DataStoreNode<?> node, int timeoutDuration) {
        finalDecisionTimeout = node.getContext().system().scheduler().scheduleOnce(
                Duration.ofSeconds(timeoutDuration), // delay
                node.getSelf(), // receiver
                new TimeoutExpired(uuid, TIMEOUT_TYPE.FINALDECISION_RESPONSE_MISSING), // message
                node.getContext().dispatcher(), // executor
                node.getSelf()); // sender
    }

    public void startVoteRequestTimeout(DataStoreNode<?> node, int timeoutDuration) {
        voteRequestTimeout = node.getContext().system().scheduler().scheduleOnce(
                Duration.ofSeconds(timeoutDuration), // delay
                node.getSelf(), // receiver
                new TimeoutExpired(uuid, TIMEOUT_TYPE.VOTE_REQUEST_MISSING), // message
                node.getContext().dispatcher(), // executor
                node.getSelf()); // sender
    }

    public void startVoteResponseTimeout(DataStoreNode<?> node, int timeoutDuration) {
        voteResponseTimeout = node.getContext().system().scheduler().scheduleOnce(
                Duration.ofSeconds(timeoutDuration), // delay
                node.getSelf(), // receiver
                new TimeoutExpired(uuid, TIMEOUT_TYPE.VOTE_RESPONSE_MISSING), // message
                node.getContext().dispatcher(), // executor
                node.getSelf()); // sender
    }

    /**
     * Cancel timer based on its type.
     *
     * @param type {@link TIMEOUT_TYPE}
     */
    public void cancelTimer(TIMEOUT_TYPE type) {

        switch (type) {
            case VOTE_REQUEST_MISSING:
                this.voteRequestTimeout.cancel();
                break;
            case FINALDECISION_RESPONSE_MISSING:
                this.finalDecisionTimeout.cancel();
                break;
            case VOTE_RESPONSE_MISSING:
                this.voteResponseTimeout.cancel();
                break;
        }
    }

    /**
     * @return Whether the transaction of this context has already been committed or aborted.
     */
    public abstract boolean isDecided();
}
