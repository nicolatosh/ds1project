package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorRef;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Dispatcher {
    private final Map<Integer, ActorRef> map;

    public Dispatcher() {
        this.map = new HashMap<>();
    }

    public void map(ActorRef actorRef, int key) {
        map.put(key, actorRef);
    }

    public ActorRef byKey(int key) {
        return map.get(key);
    }

    public List<ActorRef> all() {
        return new ArrayList<>(map.values());
    }
}
