package it.unitn.arpino.ds1project.messages.server;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.List;

/**
 * A message that is used to provide to a server the list of other servers in the Data Store.
 *
 * @see Server
 */
public class ServerStartMsg extends Message {
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
    public Message.TYPE getType() {
        return Message.TYPE.NodeControl;
    }
}
