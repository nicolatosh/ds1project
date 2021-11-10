package it.unitn.arpino.ds1project.simulation;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.messages.Message;

import java.util.Set;

public class Multicast {
    private final ActorRef sender;
    private final Set<ActorRef> receivers;
    private final Message message;
    final double crashP;

    /**
     * @param sender    Sender of this multicast
     * @param receivers Intended receivers of this multicast
     * @param message   Message of this multicast
     * @param crashP    Probability that a crash happens when sending the message to a receiver. If a crash happens,
     *                  all the remaining receivers are not contacted.
     */
    public Multicast(ActorRef sender, Set<ActorRef> receivers, Message message, double crashP) {
        this.sender = sender;
        this.receivers = receivers;
        this.message = message;
        this.crashP = crashP;
    }

    /**
     * Starts the multicast.
     *
     * @return Whether the multicast completed successfully (the message has been sent to all the intended receivers),
     * or it has not due to a crash that has been simulated.
     */
    public boolean multicast() {
        for (ActorRef receiver : receivers) {
            if (Math.random() < crashP) {
                return false;
            }
            receiver.tell(message, sender);
        }
        return true;
    }
}
