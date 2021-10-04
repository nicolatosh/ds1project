package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.TYPE;
import it.unitn.arpino.ds1project.messages.Transactional;
import it.unitn.arpino.ds1project.messages.Typed;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;

import java.io.Serializable;
import java.util.UUID;

/**
 * A message that a client uses to request a READ to a coordinator.
 *
 * @see TxnClient
 * @see Coordinator
 */
public class ReadMsg implements Typed, Transactional, Serializable {
    private final UUID uuid;
    /**
     * The key of the data item that the client wishes to read
     *
     * @see TxnClient
     */
    public final int key;


    public ReadMsg(UUID uuid, int key) {
        this.uuid = uuid;
        this.key = key;
    }

    @Override
    public TYPE getType() {
        return TYPE.Conversational;
    }

    @Override
    public UUID uuid() {
        return uuid;
    }
}
