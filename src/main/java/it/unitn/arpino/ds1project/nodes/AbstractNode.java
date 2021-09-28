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
        public final List<ActorRef> participants;

        public StartMessage(List<ActorRef> participants) {
            this.participants = List.copyOf(participants);
        }
    }


    AbstractViewManager<?> viewManager;

    protected final int id;
    protected final List<ActorRef> participants;

    public AbstractNode(int id) {
        this.id = id;
        participants = new ArrayList<>();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartMessage.class, this::setParticipants)
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

    private void setParticipants(StartMessage msg) {
        for (ActorRef actorRef : msg.participants) {
            if (!actorRef.equals(getSelf())) {
                participants.add(actorRef);
            }
        }
    }

    protected void multicast(Serializable msg) {
        for (ActorRef participant : participants) {
            participant.tell(msg, getSelf());
        }
    }
}
