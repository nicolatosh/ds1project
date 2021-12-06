package it.unitn.arpino.ds1project.nodes.context;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import it.unitn.arpino.ds1project.messages.TimeoutMsg;

import java.time.Duration;
import java.util.Objects;
import java.util.UUID;

/**
 * Base class which is used to encapsulate everything related to a transaction.
 */
public abstract class RequestContext {
    public final UUID uuid;
    public final ActorRef subject;

    private Cancellable timer;

    /**
     * Creates a new context with the specified identifier.
     *
     * @param uuid    Unique identifier for this context.
     * @param subject The subject of this context.
     */
    public RequestContext(UUID uuid, ActorRef subject) {
        this.uuid = uuid;
        this.subject = subject;
    }

    /**
     * @return Whether the outcome of the transaction has already been determined.
     */
    public abstract boolean isDecided();

    /**
     * Starts a countdown timer. When the timeout expires, it sends a {@link TimeoutMsg} to the DataStoreNode itself.
     */
    public void startTimer(AbstractActor node, int duration) {
        if (timer != null) {
            cancelTimer();
        }
        timer = node.getContext().system().scheduler().scheduleOnce(
                Duration.ofSeconds(duration), // delay
                node.getSelf(), // receiver
                new TimeoutMsg(uuid), // message
                node.getContext().dispatcher(), // executor
                node.getSelf()); // sender
    }

    /**
     * Cancels the timer.
     */
    public void cancelTimer() {
        if (timer != null) {
            timer.cancel();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RequestContext)) return false;
        RequestContext that = (RequestContext) o;
        return uuid.equals(that.uuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uuid);
    }
}
