package it.unitn.arpino.ds1project.messages.client;

import it.unitn.arpino.ds1project.messages.TYPE;
import it.unitn.arpino.ds1project.messages.Transactional;
import it.unitn.arpino.ds1project.messages.Typed;

import java.io.Serializable;
import java.util.UUID;

/**
 * Reply from the coordinator when requested a READ on a given key
 */
public class ReadResultMsg implements Typed, Transactional, Serializable {
    private final UUID uuid;
    public final int key;
    public final int value;

    /**
     * @param key   The key associated to the requested item
     * @param value The value found in the data store for that item
     */
    public ReadResultMsg(UUID uuid, int key, int value) {
        this.uuid = uuid;
        this.key = key;
        this.value = value;
    }

    @Override
    public TYPE getType() {
        return TYPE.Conversational;
    }

    public UUID uuid() {
        return uuid;
    }
}
