package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.MessageType;

import java.io.Serializable;

/**
 * WRITE request from the client to the coordinator
 */
public class WriteMsg implements MessageType, Serializable {
    public final int key;
    public final int value;

    /**
     * @param key   The key of the value to write
     * @param value The new value to write
     */
    public WriteMsg(int key, int value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public TYPE getType() {
        return TYPE.Conversational;
    }
}
