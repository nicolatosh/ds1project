package it.unitn.arpino.ds1project.nodes;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;

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

    protected final int id;
    protected final List<ActorRef> participants;

    public AbstractNode(int id) {
        this.id = id;
        this.participants = new ArrayList<>();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartMessage.class, this::setParticipants)
                .build();
    }

    private void setParticipants(StartMessage msg) {
        for (ActorRef actorRef : msg.participants) {
            if (!actorRef.equals(getSelf())) {
                this.participants.add(actorRef);
            }
        }
    }

    protected void multicast(Serializable msg) {
        for (ActorRef participant : participants) {
            participant.tell(msg, getSelf());
        }
    }
}
