package it.unitn.arpino.ds1project.nodes.context;

import akka.actor.AbstractActor;
import akka.actor.Cancellable;
import it.unitn.arpino.ds1project.messages.TimeoutExpired;

import java.time.Duration;
import java.util.UUID;

public abstract class RequestContext {
    public enum Status {
        ACTIVE,
        COMPLETED,
        EXPIRED
    }

    public final UUID uuid;

    protected Status status;

    private Cancellable timeout;

    boolean crashed;

    public RequestContext(UUID uuid) {
        this.uuid = uuid;
        status = Status.ACTIVE;
    }

    public void startTimer(AbstractActor actor, int timeout_duration) {
        timeout = actor.getContext().system().scheduler().scheduleOnce(
                Duration.ofSeconds(timeout_duration), // delay
                actor.getSelf(), // receiver
                new TimeoutExpired(uuid), // message
                actor.getContext().dispatcher(), // executor
                actor.getSelf()); // sender
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
