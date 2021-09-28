package it.unitn.arpino.ds1project.nodes;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.transaction.messages.TxnBeginMsg;
import it.unitn.arpino.ds1project.twopc.messages.AbstractTwoPcMessage;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractNode extends AbstractActor {
    public static class StartMessage implements Serializable {
        public final List<ActorRef> group;

        public StartMessage(List<ActorRef> group) {
            this.group = List.copyOf(group);
        }
    }


    AbstractViewManager<?> viewManager;

    protected final int id;
    protected final List<ActorRef> group;

    public AbstractNode(int id) {
        this.id = id;
        group = new ArrayList<>();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartMessage.class, this::setGroup)
                .build();
    }

    @Override
    public void aroundReceive(PartialFunction<Object, BoxedUnit> receive, Object msg) {
        if (msg instanceof AbstractTwoPcMessage) {
            AbstractTwoPcMessage message = (AbstractTwoPcMessage) msg;
            viewManager.changeView(message.txn);
        }

        super.aroundReceive(receive, msg);

        if (msg instanceof TxnBeginMsg) {
            viewManager.sync();
        }
    }

    private void setGroup(StartMessage msg) {
        for (ActorRef actorRef : msg.group) {
            if (!actorRef.equals(getSelf())) {
                group.add(actorRef);
            }
        }
    }

    protected void multicast(Serializable msg) {
        for (ActorRef node : group) {
            node.tell(msg, getSelf());
        }
    }
}
