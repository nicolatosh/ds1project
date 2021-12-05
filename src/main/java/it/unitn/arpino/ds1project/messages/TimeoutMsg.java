package it.unitn.arpino.ds1project.messages;

import akka.dispatch.ControlMessage;

import java.util.UUID;


public class TimeoutMsg extends TxnMessage implements ControlMessage {
    public TimeoutMsg(UUID uuid) {
        super(uuid);
    }

    @Override
    public String toString() {
        return "VoteRequestTimeout{" +
                "uuid=" + uuid +
                '}';
    }
}
