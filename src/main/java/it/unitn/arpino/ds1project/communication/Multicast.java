package it.unitn.arpino.ds1project.communication;

import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.Set;

public class Multicast {
    private final ActorRef sender;
    private final Set<ActorRef> receivers;
    private final Serializable msg;

    public Multicast(ActorRef sender, Set<ActorRef> receivers, Serializable msg) {
        this.sender = sender;
        this.receivers = receivers;
        this.msg = msg;
    }

    public void multicast() {
        receivers.forEach(actorRef -> actorRef.tell(msg, sender));
    }
}
