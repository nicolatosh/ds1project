package it.unitn.arpino.ds1project.nodes.context;

import akka.actor.Cancellable;
import it.unitn.arpino.ds1project.messages.TimeoutExpired;
import it.unitn.arpino.ds1project.nodes.DataStoreNode;

import java.time.Duration;
import java.util.UUID;

public abstract class RequestContext {
    public final UUID uuid;

    private boolean completed;

    private Cancellable timeout;

    boolean crashed;

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

    public boolean isCompleted() {
        return completed;
    }

    public void setCompleted() {
        completed = true;
    }

    public void setCrashed() {
        crashed = true;
    }

    public boolean wasCrashed() {
        return crashed;
    }
}
