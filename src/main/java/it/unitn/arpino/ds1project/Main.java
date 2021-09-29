package it.unitn.arpino.ds1project;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;

import java.util.ArrayList;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("ds1project");

        final ActorRef coord1 = system.actorOf(Coordinator.props(), "coord1");

        final ActorRef client1 = system.actorOf(TxnClient.props(1), "client1");

        final List<ActorRef> coordinators = new ArrayList<>();
        coordinators.add(coord1);

        client1.tell(new TxnClient.WelcomeMsg(9, coordinators), ActorRef.noSender());

        system.terminate();
    }
}
