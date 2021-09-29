package it.unitn.arpino.ds1project.messages.server;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.messages.MessageType;

import java.io.Serializable;

public class ReadRequest implements MessageType, Serializable {
    public final ActorRef client;
    public final int key;

    public ReadRequest(ActorRef client, int key) {
        this.client = client;
        this.key = key;
    }

    @Override
    public TYPE getType() {
        return TYPE.Internal;
    }
}
