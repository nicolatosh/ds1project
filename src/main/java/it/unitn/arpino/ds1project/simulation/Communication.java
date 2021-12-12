package it.unitn.arpino.ds1project.simulation;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.messages.TxnMessage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class Communication {
    public static NetworkParameters parameters = new NetworkParameters();

    private ActorRef sender;
    private final List<ActorRef> receivers;
    private TxnMessage message;
    private double successP;

    private Communication() {
        receivers = new ArrayList<>();
    }

    public static Communication builder() {
        return new Communication();
    }

    /**
     * @param sender Sender of the communication.
     */
    public Communication ofSender(ActorRef sender) {
        this.sender = sender;
        return this;
    }

    /**
     * @param receiver Intended receiver of this unicast
     */
    public Communication ofReceiver(ActorRef receiver) {
        receivers.add(receiver);
        return this;
    }

    /**
     * @param receivers Intended receivers of this multicast
     */
    public Communication ofReceivers(Collection<ActorRef> receivers) {
        this.receivers.addAll(receivers);
        return this;
    }

    public Communication ofMessage(TxnMessage message) {
        this.message = message;
        return this;
    }

    /**
     * @param successP Probability that the multicast completes successfully; i.e., no crash occurs and all the messages
     *                 are sent to the intended receivers.
     */
    public Communication ofSuccessProbability(double successP) {
        this.successP = successP;
        return this;
    }

    /**
     * @return the probability that a crash should occur in a single tell of a multicast,
     * so that the probability that the multicast completes correctly is successP.
     */
    private double crashP() {
        // (1 - crashP)^nParticipants = successP
        // (1 - crashP) = sqrt_nParticipants(successP)
        // crashP = 1 - sqrt_nParticipants(successP)
        // crashP = 1 - successP^(1/nParticipants)
        return 1 - Math.pow(successP, 1. / receivers.size());
    }

    /* @return Whether the multicast (or unicast) completed successfully (the message has been sent to all the intended receivers),
     * or it has not due to a crash that has been simulated.
     */
    public boolean run() {
        double crashP = crashP();

        Iterator<ActorRef> iterator = receivers.iterator();
        while (iterator.hasNext()) {
            if (Math.random() < crashP) {
                return false;
            }

            if (parameters.simulateNetworkDelays) {
                try {
                    var delayMs = ThreadLocalRandom.current().nextLong(parameters.minimumNetworkDelayMs, parameters.maximumNetworkDelayMs);
                    //noinspection BusyWait
                    Thread.sleep(delayMs);
                } catch (InterruptedException ignored) {
                }
            }

            ActorRef receiver = iterator.next();
            receiver.tell(message, sender);

            iterator.remove();
        }
        return true;
    }

    /**
     * @return The receivers to which the message has not been sent, due to the simulation of a crash.
     */
    public Collection<ActorRef> getMissing() {
        return receivers;
    }
}
