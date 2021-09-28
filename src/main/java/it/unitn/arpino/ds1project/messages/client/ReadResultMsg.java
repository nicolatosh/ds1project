package it.unitn.arpino.ds1project.messages.client;

import it.unitn.arpino.ds1project.messages.MessageType;

import java.io.Serializable;

/**
 * Reply from the coordinator when requested a READ on a given key
 */
public class ReadResultMsg implements MessageType, Serializable {
    public final int key;
    public final int value;

    /**
     * @param key   The key associated to the requested item
     * @param value The value found in the data store for that item
     */
    public ReadResultMsg(int key, int value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public TYPE getType() {
        return TYPE.Conversational;
    }
}
