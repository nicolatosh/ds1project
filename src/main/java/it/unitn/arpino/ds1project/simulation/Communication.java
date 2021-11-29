package it.unitn.arpino.ds1project.simulation;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.messages.TxnMessage;

import java.util.*;

public class Communication {
    public static final int MINIMUM_NETWORK_DELAY = 5; //ms
    public static final int MAXIMUM_NETWORK_DELAY = 10; //ms
    private ActorRef sender;
    private final List<ActorRef> receivers;
    private TxnMessage message;
    private double crashP;
    private final Random random;

    private Communication() {
        receivers = new ArrayList<>();
        this.random = new Random();
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
     * @param crashP Probability that a crash happens when sending the message to a receiver.
     *               If a crash happens, all the remaining receivers are not contacted.
     */
    public Communication ofCrashProbability(double crashP) {
        this.crashP = crashP;
        return this;
    }

    /* @return Whether the multicast (or unicast) completed successfully (the message has been sent to all the intended receivers),
     * or it has not due to a crash that has been simulated.
     */
    public boolean run() {
        Iterator<ActorRef> iterator = receivers.iterator();
        while (iterator.hasNext()) {
            if (Math.random() < crashP) {
                return false;
            }
            ActorRef receiver = iterator.next();
            try {
                Thread.sleep(this.random.nextInt(MAXIMUM_NETWORK_DELAY) + MINIMUM_NETWORK_DELAY);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
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
