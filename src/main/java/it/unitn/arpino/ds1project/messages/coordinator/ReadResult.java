package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.TYPE;
import it.unitn.arpino.ds1project.messages.Transactional;
import it.unitn.arpino.ds1project.messages.Typed;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.io.Serializable;
import java.util.UUID;

/**
 * A message that a server sends to a coordinator containing the result of the READ operation it had requested
 * on behalf of a client.
 *
 * @see Coordinator
 * @see Server
 * @see TxnClient
 */
public class ReadResult implements Typed, Transactional, Serializable {
    private final UUID uuid;

    /**
     * The key that the client requested to read.
     *
     * @see TxnClient
     */
    public final int key;

    /**
     * The value that the server has read.
     *
     * @see Server
     */
    public final int value;

    /**
     * @param key   The key that the client requested to read.
     * @param value The value which has been read.
     */
    public ReadResult(UUID uuid, int key, int value) {
        this.uuid = uuid;
        this.key = key;
        this.value = value;
    }

    @Override
    public TYPE getType() {
        return TYPE.Internal;
    }

    @Override
    public UUID uuid() {
        return uuid;
    }
}
