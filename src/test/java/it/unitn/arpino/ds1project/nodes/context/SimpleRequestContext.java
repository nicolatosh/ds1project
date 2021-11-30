package it.unitn.arpino.ds1project.nodes.context;

import akka.actor.ActorRef;

import java.util.UUID;

public class SimpleRequestContext extends RequestContext {
    private boolean decided;

    public SimpleRequestContext(UUID uuid, ActorRef subject) {
        super(uuid, subject);
    }


    @Override
    public boolean isDecided() {
        return decided;
    }

    public void setDecided() {
        decided = true;
    }
}
