package it.unitn.arpino.ds1project.messages.coordinator;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.List;

/**
 * A message that is used to provide to a coordinator the list of servers in the DataStore,
 * and the keys that each server is responsible for.
 *
 * @see Coordinator
 * @see Server
 */
public class CoordinatorStartMsg extends Message {
    /**
     * Associates a server with the keys it is responsible for.
     *
     * @see Server
     */
    public static class ServerInfo {
        public final ActorRef server;
        public final int lowerKey, upperKey;

        public ServerInfo(ActorRef server, int lowerKey, int upperKey) {
            this.server = server;
            this.lowerKey = lowerKey;
            this.upperKey = upperKey;
        }
    }


    public final List<ServerInfo> serverInfos;

    public CoordinatorStartMsg(List<ServerInfo> serverInfos) {
        this.serverInfos = List.copyOf(serverInfos);
    }

    @Override
    public Message.TYPE getType() {
        return Message.TYPE.NodeControl;
    }
}
