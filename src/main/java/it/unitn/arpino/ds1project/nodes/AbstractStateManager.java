package it.unitn.arpino.ds1project.nodes;

import akka.actor.AbstractActor;


public abstract class AbstractStateManager<T extends AbstractActor> {
    T node;

    public AbstractStateManager(T node) {
        this.node = node;
    }

    protected void before() {
    }

    protected void after() {
    }
}
