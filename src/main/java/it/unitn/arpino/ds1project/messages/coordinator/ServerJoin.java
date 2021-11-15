package it.unitn.arpino.ds1project.messages.coordinator;

import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

/**
 * A message used to signal to a {@link Coordinator} that a new {@link Server} joined the distributed Data Store, and
 * containing the keys of the data items of the server.
 */
public class ServerJoin extends Message {
    public final int lowerKey, upperKey;

    public ServerJoin(int lowerKey, int upperKey) {
        super(null);
        this.lowerKey = lowerKey;
        this.upperKey = upperKey;
    }

    @Override
    public Type getType() {
        return Type.Setup;
    }
}
