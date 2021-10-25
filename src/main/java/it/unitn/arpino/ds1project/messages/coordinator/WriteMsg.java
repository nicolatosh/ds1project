package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.messages.Transactional;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;

import java.util.UUID;

/**
 * A message that a client uses to request a WRITE to a coordinator
 *
 * @see TxnClient
 * @see Coordinator
 */
public class WriteMsg extends Message implements Transactional {
    private final UUID uuid;

    /**
     * The key that the client wishes to write
     *
     * @see TxnClient
     */
    public final int key;

    /**
     * The value that the client wishes to write
     *
     * @see TxnClient
     */
    public final int value;


    public WriteMsg(UUID uuid, int key, int value) {
        this.uuid = uuid;
        this.key = key;
        this.value = value;
    }

    @Override
    public Message.TYPE getType() {
        return Message.TYPE.Conversational;
    }

    @Override
    public UUID uuid() {
        return uuid;
    }
}
