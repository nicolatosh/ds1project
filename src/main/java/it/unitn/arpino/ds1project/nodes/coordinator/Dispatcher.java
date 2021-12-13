package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.messages.server.ReadRequest;
import it.unitn.arpino.ds1project.messages.server.WriteRequest;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.*;

/**
 * Holds the mappings between keys and the {@link ActorRef}s of the {@link Server}s in the distributed Data Store which
 * store the data items with those key, and to which the {@link Coordinator} can send {@link ReadRequest}s and
 * {@link WriteRequest}s.
 */
public class Dispatcher {
    private final Map<Integer, ActorRef> map;

    public Dispatcher() {
        this.map = new HashMap<>();
    }

    /**
     * Adds a mapping for the specified key and ActorRef.
     */
    public void map(int key, ActorRef actorRef) {
        map.put(key, actorRef);
    }

    /**
     * @param key The key of the data item for which a reference to the {@link Server} storing that data item is wanted.
     * @return The {@link Server} in the distributed Data Store which stores the data item with the specified key.
     */
    public Optional<ActorRef> getServer(int key) {
        if (map.containsKey(key)) {
            return Optional.of(map.get(key));
        }
        return Optional.empty();
    }

    public Collection<ActorRef> getAll() {
        return new HashSet<>(map.values());
    }
}
