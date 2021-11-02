package it.unitn.arpino.ds1project.nodes.context;

import akka.actor.Cancellable;
import it.unitn.arpino.ds1project.messages.TimeoutExpired;
import it.unitn.arpino.ds1project.nodes.DataStoreNode;

import java.time.Duration;
import java.util.UUID;

public abstract class RequestContext {
    public final UUID uuid;

    private Cancellable timeout;

    public RequestContext(UUID uuid) {
        this.uuid = uuid;
    }

    /**
     * Starts a countdown timer. If the timer is not canceled before expiring, sends a {@link TimeoutExpired} message
     * to the node which started the timer.
     *
     * @param node            The node which starts the timer.
     * @param timeoutDuration Duration in seconds of the timer.
     */
    public void startTimer(DataStoreNode<?> node, int timeoutDuration) {
        timeout = node.getContext().system().scheduler().scheduleOnce(
                Duration.ofSeconds(timeoutDuration), // delay
                node.getSelf(), // receiver
                new TimeoutExpired(uuid), // message
                node.getContext().dispatcher(), // executor
                node.getSelf()); // sender
    }

    /**
     * Cancels the timer.
     */
    public void cancelTimer() {
        timeout.cancel();
    }

    /**
     * @return Whether this context is decided. A context is decided when the related transaction committed or
     * aborted.
     */
    public abstract boolean isDecided();
}
