package it.unitn.arpino.ds1project.communication;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Multicast {
    protected ActorRef sender;
    protected List<ActorRef> receivers;
    // A value of -1 means that the sender never crashes,
    // and thus correctly completes the multicast.
    protected int round = -1;
    Duration duration;

    Serializable msg;

    public Multicast() {
        receivers = new ArrayList<>();
    }

    public Multicast setSender(ActorRef sender) {
        this.sender = sender;
        return this;
    }

    public Multicast addReceiver(ActorRef receiver) {
        receivers.add(receiver);
        return this;
    }

    public Multicast addReceivers(List<ActorRef> receivers) {
        this.receivers.addAll(receivers);
        return this;
    }

    public Multicast shuffle() {
        Collections.shuffle(receivers);
        return this;
    }

    /**
     * @param round The round at which the multicast will crash.
     *              A value of 0 means that the sender crashes before sending the first message.
     *              A value of 'i' means that the sender correctly sends the first i-1 messages,
     *              and then crashes.
     */
    public Multicast setCrushingRound(int round) {
        this.round = round;
        return this;
    }

    public void withDelay(long seconds) {
        duration = Duration.ofSeconds(seconds);
    }

    public Multicast setMessage(Serializable msg) {
        this.msg = msg;
        return this;
    }

    public void multicast() {
        if (round == -1) {
            round = receivers.size();
        }

        receivers.subList(0, round)
                .forEach(receiver -> {
                    receiver.tell(msg, sender);
                });
    }

    /**
     * @return Whether the multicast has completed correctly,
     * meaning that the message was sent to all intended receivers
     */
    public boolean completed() {
        return this.round == -1;
    }
}
