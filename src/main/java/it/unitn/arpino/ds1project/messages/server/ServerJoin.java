package it.unitn.arpino.ds1project.messages.server;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.nodes.server.Server;

/**
 * A message used to signal to a {@link Server} that a new Server joined the distributed Data Store.
 */
public class ServerJoin extends Message {
    public final ActorRef server;

    public ServerJoin(ActorRef server) {
        super(null);
        this.server = server;
    }

    @Override
    public Message.Type getType() {
        return Message.Type.Setup;
    }
}
