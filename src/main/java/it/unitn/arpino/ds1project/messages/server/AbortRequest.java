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
 * A message that a coordinator sends to a server on behalf of a client to abort an ongoing transaction.
 *
 * @see Coordinator
 * @see Server
 * @see TxnClient
 */
public class AbortRequest implements Typed, Transactional, Serializable {
    private final UUID uuid;

    public AbortRequest(UUID uuid) {
        this.uuid = uuid;
    }

    @Override
    public TYPE getType() {
        return TYPE.Internal;
    }

    @Override
    public UUID uuid() {
        return null;
    }
}
