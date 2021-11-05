package it.unitn.arpino.ds1project.messages.server;

import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.UUID;

/**
 * A message that a {@link Coordinator} uses to request a read to a {@link Server}.
 */
public class ReadRequest extends Message {
    public final int key;

    public ReadRequest(UUID uuid, int key) {
        super(uuid);
        this.key = key;
    }

    @Override
    public Type getType() {
        return Type.Internal;
    }
}
