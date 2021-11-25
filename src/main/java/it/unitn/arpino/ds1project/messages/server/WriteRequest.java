package it.unitn.arpino.ds1project.messages.server;

import it.unitn.arpino.ds1project.messages.TxnMessage;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.UUID;

/**
 * A message that a {@link Coordinator} uses to request a write to a {@link Server}.
 */
public class WriteRequest extends TxnMessage {
    public final int key;
    public final int value;

    public WriteRequest(UUID uuid, int key, int value) {
        super(uuid);
        this.key = key;
        this.value = value;
    }
}
