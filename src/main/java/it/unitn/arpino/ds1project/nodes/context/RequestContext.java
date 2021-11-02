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

    public void startTimer(DataStoreNode<?> node, int timeoutDuration) {
        timeout = node.getContext().system().scheduler().scheduleOnce(
                Duration.ofSeconds(timeoutDuration), // delay
                node.getSelf(), // receiver
                new TimeoutExpired(uuid), // message
                node.getContext().dispatcher(), // executor
                node.getSelf()); // sender
    }

    public void cancelTimer() {
        timeout.cancel();
    }

    /**
     * @return Whether this context is completed. A context is completed when the related transaction committed or
     * aborted.
     */
    public abstract boolean isCompleted();
}
