package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.MessageType;

import java.io.Serializable;

/**
 * READ request from the client to the coordinator
 */
public class ReadMsg implements MessageType, Serializable {
    public final int key;

    /**
     * @param key The key of the value to read
     */
    public ReadMsg(int key) {
        this.key = key;
    }

    @Override
    public TYPE getType() {
        return TYPE.Conversational;
    }
}
