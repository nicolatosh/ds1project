package it.unitn.arpino.ds1project.messages;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.nodes.server.Server;

/**
 * Associates a server with the keys it is responsible for.
 *
 * @see Server
 */
public class ServerInfo extends Message {
    public final ActorRef server;
    public final int lowerKey, upperKey;

    public ServerInfo(ActorRef server, int lowerKey, int upperKey) {
        this.server = server;
        this.lowerKey = lowerKey;
        this.upperKey = upperKey;
    }

    @Override
    public TYPE getType() {
        return TYPE.NodeControl;
    }
}
