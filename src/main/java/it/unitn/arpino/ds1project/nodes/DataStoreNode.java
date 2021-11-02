package it.unitn.arpino.ds1project.nodes;

import akka.japi.pf.ReceiveBuilder;

public abstract class DataStoreNode extends AbstractNode {
    public enum Status {
        ALIVE,
        CRASHED
    }

    private Status status;

    public DataStoreNode() {
        status = DataStoreNode.Status.ALIVE;
    }

    Status getStatus() {
        return status;
    }

    protected void crash() {
        getContext().become(new ReceiveBuilder()
                .matchAny(msg -> {
                    // this suppresses Dead Letter warnings.
                }).build());
        status = DataStoreNode.Status.CRASHED;
    }

    protected void resume() {
        getContext().become(createReceive());
        status = DataStoreNode.Status.ALIVE;
    }
}
