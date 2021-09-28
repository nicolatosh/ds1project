package it.unitn.arpino.ds1project.transaction.messages;

import java.io.Serializable;

/**
 * WRITE request from the client to the coordinator
 */
public class WriteMsg implements Serializable {
    public final int clientId;
    public final int key;
    public final int value;

    /**
     * @param clientId
     * @param key      The key of the value to write
     * @param value    The new value to write
     */
    public WriteMsg(int clientId, int key, int value) {
        this.clientId = clientId;
        this.key = key;
        this.value = value;
    }
}
