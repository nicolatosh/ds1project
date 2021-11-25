package it.unitn.arpino.ds1project.nodes;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.messages.JoinMessage;
import it.unitn.arpino.ds1project.nodes.context.SimpleRequestContext;

public class SimpleNode extends DataStoreNode<SimpleRequestContext> {
    public static Props props() {
        return Props.create(SimpleNode.class, SimpleNode::new);
    }

    @Override
    protected Receive createAliveReceive() {
        return ReceiveBuilder.create()
                .matchEquals("Hello", msg -> getSender().tell("World", getSelf()))
                .build();
    }

    @Override
    protected void onJoinMessage(JoinMessage msg) {
    }
}


