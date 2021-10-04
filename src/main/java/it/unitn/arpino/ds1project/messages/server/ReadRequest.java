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
 * A message that a coordinator sends to a server requesting a READ operation on behalf of a client.
 *
 * @see Coordinator
 * @see Server
 * @see TxnClient
 */
public class ReadRequest implements Typed, Transactional, Serializable {
    private final UUID uuid;

    /**
     * The key that the client requested to read.
     *
     * @see TxnClient
     */
    public final int key;

    public ReadRequest(UUID uuid, int key) {
        this.uuid = uuid;
        this.key = key;
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
