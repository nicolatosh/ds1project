package it.unitn.arpino.ds1project.messages.server;

import it.unitn.arpino.ds1project.messages.TYPE;
import it.unitn.arpino.ds1project.messages.Transactional;
import it.unitn.arpino.ds1project.messages.Typed;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.io.Serializable;
import java.util.UUID;

/**
 * A message that a coordinator sends to a server requesting a WRITE operation
 * on behalf of a client.
 *
 * @see Coordinator
 * @see Server
 * @see TxnClient
 */
public class WriteRequest implements Typed, Transactional, Serializable {
    private final UUID uuid;

    /**
     * The key that the client requested to write.
     *
     * @see TxnClient
     */
    public final int key;

    /**
     * The value that the client requested to write.
     *
     * @see TxnClient
     */
    public final int value;

    public WriteRequest(UUID uuid, int key, int value) {
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