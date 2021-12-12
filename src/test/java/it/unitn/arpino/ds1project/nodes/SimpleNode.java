package it.unitn.arpino.ds1project.nodes;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.messages.StartMessage;
import it.unitn.arpino.ds1project.nodes.context.SimpleRequestContext;

public class SimpleNode extends DataStoreNode<SimpleRequestContext> {
    public static Props props() {
        return Props.create(SimpleNode.class, SimpleNode::new);
    }


    @Override
    public Receive createReceive() {
        return ReceiveBuilder.create()
                .match(StartMessage.class, this::onStartMsg)
                .matchAny(msg -> {
                    logger.info("Stashed " + msg + " from " + getSender().path().name());
                    stash();
                })
                .build();
    }

    @Override
    protected Receive createAliveReceive() {
        return ReceiveBuilder.create()
                .matchEquals("Hello", msg -> getSender().tell("World", getSelf()))
                .build();
    }

    private void onStartMsg(StartMessage msg) {
        logger.info("Starting");
        getContext().become(createAliveReceive());
        unstashAll();
    }
}


