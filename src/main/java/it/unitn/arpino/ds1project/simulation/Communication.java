package it.unitn.arpino.ds1project.simulation;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.messages.TxnMessage;

import java.util.Collection;

public class Communication {
    /**
     * Multicasts a message to a set of receivers.
     *
     * @param sender    Sender of this multicast
     * @param receivers Intended receivers of this multicast
     * @param message   Message of this multicast
     * @param crashP    Probability that a crash happens when sending the message to a receiver.
     *                  If a crash happens, all the remaining receivers are not contacted.
     * @return Whether the multicast completed successfully (the message has been sent to all the intended receivers),
     * or it has not due to a crash that has been simulated.
     */
    public static boolean multicast(ActorRef sender, Collection<ActorRef> receivers, TxnMessage message, double crashP) {
        return receivers.stream().allMatch(receiver -> unicast(sender, receiver, message, crashP));
    }

    /**
     * Unicasts a message to a receiver.
     *
     * @param sender   Sender of this unicast
     * @param receiver Intended receiver of this unicast
     * @param message  Message of this unicast
     * @param crashP   Probability that a crash happens when sending the message to the receiver.
     *                 If the crash happens, the receiver is not contacted.
     * @return Whether the unicast completed successfully (the message has been sent to the intended receiver),
     * or it has not due to a crash that has been simulated.
     */
    public static boolean unicast(ActorRef sender, ActorRef receiver, TxnMessage message, double crashP) {
        if (Math.random() < crashP) {
            return false;
        }
        receiver.tell(message, sender);
        return true;
    }
}
