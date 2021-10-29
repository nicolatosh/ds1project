package it.unitn.arpino.ds1project.nodes.server;

import akka.actor.ActorRef;

import java.io.Serializable;

/**
 * Associates a server with the keys it is responsible for.
 *
 * @see Server
 */
public class ServerInfo implements Serializable {
    public final ActorRef server;
    public final int lowerKey, upperKey;

    public ServerInfo(ActorRef server, int lowerKey, int upperKey) {
        this.server = server;
        this.lowerKey = lowerKey;
        this.upperKey = upperKey;
    }
}
