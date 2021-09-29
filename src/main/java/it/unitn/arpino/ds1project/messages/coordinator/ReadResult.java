package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.MessageType;

import java.io.Serializable;

public class ReadResult implements MessageType, Serializable {
    public final int key;

    public ReadResult(int key) {
        this.key = key;
    }

    @Override
    public TYPE getType() {
        return TYPE.Internal;
    }
}
