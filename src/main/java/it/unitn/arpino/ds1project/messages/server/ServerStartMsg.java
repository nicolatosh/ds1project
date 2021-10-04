package it.unitn.arpino.ds1project.messages.server;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.messages.TYPE;
import it.unitn.arpino.ds1project.messages.Typed;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.io.Serializable;
import java.util.List;

/**
 * A message that is used to provide to a server the list of other servers in the Data Store.
 *
 * @see Server
 */
public class ServerStartMsg implements Typed, Serializable {
    /**
     * The other servers in the Data Store.
     *
     * @see Server
     */
    public final List<ActorRef> servers;

    public ServerStartMsg(List<ActorRef> servers) {
        this.servers = List.copyOf(servers);
    }

    @Override
    public TYPE getType() {
        return TYPE.NodeControl;
    }
}
