package it.unitn.arpino.ds1project.messages.control;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.messages.MessageType;

import java.io.Serializable;
import java.util.Set;

public class StartMessage implements MessageType, Serializable {
    public final Set<ActorRef> clients;
    public final Set<ActorRef> coordinators;
    public final Set<ActorRef> servers;

    public StartMessage(Set<ActorRef> clients, Set<ActorRef> coordinators, Set<ActorRef> servers) {
        this.clients = Set.copyOf(clients);
        this.coordinators = Set.copyOf(coordinators);
        this.servers = Set.copyOf(servers);
    }

    @Override
    public TYPE getType() {
        return TYPE.NodeControl;
    }
}
