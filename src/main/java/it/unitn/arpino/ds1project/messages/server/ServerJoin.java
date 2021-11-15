package it.unitn.arpino.ds1project.messages.server;

import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.nodes.server.Server;

/**
 * A message used to signal to a {@link Server} that a new Server joined the distributed Data Store.
 */
public class ServerJoin extends Message {
    public ServerJoin() {
        super(null);
    }

    @Override
    public Message.Type getType() {
        return Message.Type.Setup;
    }
}
