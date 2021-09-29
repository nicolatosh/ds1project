package it.unitn.arpino.ds1project.messages.server;

import it.unitn.arpino.ds1project.messages.MessageType;

import java.io.Serializable;

public class WriteRequest implements MessageType, Serializable {
    public final int key;
    public final int value;

    public WriteRequest(int key, int value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public TYPE getType() {
        return TYPE.Internal;
    }
}
