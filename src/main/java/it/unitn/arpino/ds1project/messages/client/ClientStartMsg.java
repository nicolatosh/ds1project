package it.unitn.arpino.ds1project.messages.client;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.messages.TYPE;
import it.unitn.arpino.ds1project.messages.Typed;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;

import java.io.Serializable;
import java.util.List;

/**
 * A message that is used to provide to a client the list of coordinators it can contact to start a transaction,
 * and the list of keys representing the data item it can request to read and write during the transaction.
 *
 * @see TxnClient
 * @see Coordinator
 */
public class ClientStartMsg implements Typed, Serializable {
    /**
     * The coordinators that the client can contact to start a transaction.
     *
     * @see TxnClient
     * @see Coordinator
     */
    public final List<ActorRef> coordinators;

    /**
     * The keys of the data item that the client can request to read or write during a transaction.
     *
     * @see TxnClient
     */
    public final List<Integer> keys;


    public ClientStartMsg(List<ActorRef> coordinators, List<Integer> keys) {
        this.coordinators = List.copyOf(coordinators);
        this.keys = List.copyOf(keys);
    }

    @Override
    public TYPE getType() {
        return TYPE.NodeControl;
    }
}
