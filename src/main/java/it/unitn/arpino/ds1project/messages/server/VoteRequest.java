package it.unitn.arpino.ds1project.messages.server;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.messages.TxnMessage;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * A message that a {@link Coordinator} sends to a {@link Server} requesting to vote whether to commit or abort a
 * transaction.
 */
public class VoteRequest extends TxnMessage {
    public final Set<ActorRef> participants;

    public VoteRequest(UUID uuid, Collection<ActorRef> participants) {
        super(uuid);
        this.participants = new HashSet<>(participants);
    }

    @Override
    public String toString() {
        return "VoteRequest{" +
                "uuid=" + uuid +
                '}';
    }
}
