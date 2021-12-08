package it.unitn.arpino.ds1project.messages.client;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;

import java.io.Serializable;
import java.util.List;

/**
 * A message that is used to provide to a {@link TxnClient} the list of {@link Coordinator}s that are available,
 * and the list of keys of data items it can request to read or write in a transaction.
 */
public class CoordinatorList implements Serializable {
    public final List<ActorRef> coordinators;
    public final int maxKey;

    public CoordinatorList(List<ActorRef> coordinators, int maxKey) {
        this.coordinators = List.copyOf(coordinators);
        this.maxKey = maxKey;
    }

    @Override
    public String toString() {
        return "CoordinatorList{" +
                "coordinators=" + coordinators +
                ", maxKey=" + maxKey +
                '}';
    }
}
