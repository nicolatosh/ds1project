package it.unitn.arpino.ds1project.messages;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.nodes.server.Server;

/**
 * A message that carries information about the data items that a {@link Server} stores.
 */
public class ServerInfo extends Message {
    public final ActorRef server;
    public final int lowerKey, upperKey;

    public ServerInfo(ActorRef server, int lowerKey, int upperKey) {
        super(null);
        this.server = server;
        this.lowerKey = lowerKey;
        this.upperKey = upperKey;
    }

    @Override
    public Type getType() {
        return Type.Setup;
    }
}
