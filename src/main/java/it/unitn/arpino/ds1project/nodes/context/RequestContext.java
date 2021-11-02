package it.unitn.arpino.ds1project.nodes.context;

import akka.actor.Cancellable;
import it.unitn.arpino.ds1project.messages.TimeoutExpired;
import it.unitn.arpino.ds1project.nodes.AbstractNode;

import java.time.Duration;
import java.util.UUID;

public abstract class RequestContext {
    public enum Status {
        ACTIVE,
        COMPLETED
    }

    public final UUID uuid;

    protected Status status;

    private Cancellable timeout;

    boolean crashed;

    public RequestContext(UUID uuid) {
        this.uuid = uuid;
        status = Status.ACTIVE;
    }

    public void startTimer(AbstractNode node, int timeoutDuration) {
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

    public Status getStatus() {
        return status;
    }

    void setStatus(Status status) {
        this.status = status;
    }

    public void setCrashed() {
        crashed = true;
    }

    public boolean wasCrashed() {
        return crashed;
    }
}
