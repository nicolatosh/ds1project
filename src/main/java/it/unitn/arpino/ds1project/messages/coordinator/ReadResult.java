package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.messages.server.ReadRequest;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.UUID;

/**
 * A message that a {@link Server} sends to a {@link Coordinator} with the result of a {@link ReadRequest}.
 */
public class ReadResult extends Message {
    public final int key;
    public final int value;

    public ReadResult(UUID uuid, int key, int value) {
        super(uuid);
        this.key = key;
        this.value = value;
    }

    @Override
    public Type getType() {
        return Type.Internal;
    }
}
