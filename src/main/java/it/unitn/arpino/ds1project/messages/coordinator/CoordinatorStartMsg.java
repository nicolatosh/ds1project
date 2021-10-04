package it.unitn.arpino.ds1project.messages.coordinator;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.messages.TYPE;
import it.unitn.arpino.ds1project.messages.Typed;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * A message that is used to provide to a coordinator the list of servers in the DataStore,
 * and the keys that each server is responsible for.
 *
 * @see Coordinator
 * @see Server
 */
public class CoordinatorStartMsg implements Typed, Serializable {
    /**
     * Associates a server with the keys it is responsible for.
     *
     * @see Server
     */
    public final Map<ActorRef, List<Integer>> serverKeys;

    public CoordinatorStartMsg(Map<ActorRef, List<Integer>> serverKeys) {
        this.serverKeys = Map.copyOf(serverKeys);
    }

    @Override
    public TYPE getType() {
        return TYPE.NodeControl;
    }
}
