package it.unitn.arpino.ds1project.messages.client;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;

import java.util.List;

/**
 * A message that is used to provide to a client the list of coordinators it can contact to start a transaction,
 * and the list of keys representing the data item it can request to read and write during the transaction.
 *
 * @see TxnClient
 * @see Coordinator
 */
public class ClientStartMsg extends Message {
    /**
     * The coordinators that the client can contact to start a transaction.
     *
     * @see TxnClient
     * @see Coordinator
     */
    public final List<ActorRef> coordinators;

    public final int maxKey;

    public ClientStartMsg(List<ActorRef> coordinators, int maxKey) {
        this.coordinators = List.copyOf(coordinators);
        this.maxKey = maxKey;
    }

    @Override
    public Message.TYPE getType() {
        return Message.TYPE.NodeControl;
    }
}
