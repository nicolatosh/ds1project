package it.unitn.arpino.ds1project.messages.server;

import it.unitn.arpino.ds1project.messages.TxnMessage;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.UUID;

/**
 * A message that a {@link Coordinator} uses to request a read to a {@link Server}.
 */
public class ReadRequest extends TxnMessage {
    public final int key;

    public ReadRequest(UUID uuid, int key) {
        super(uuid);
        this.key = key;
    }

    @Override
    public String toString() {
        return "ReadRequest{" +
                "uuid=" + uuid +
                ", key=" + key +
                '}';
    }
}
