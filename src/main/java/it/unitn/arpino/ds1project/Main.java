package it.unitn.arpino.ds1project;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.arpino.ds1project.messages.JoinMessage;
import it.unitn.arpino.ds1project.messages.StartMessage;
import it.unitn.arpino.ds1project.messages.client.ClientStartMsg;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.List;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        final ActorSystem system = ActorSystem.create("ds1project");

        // setup servers, coordinators and clients

        final ActorRef coord1 = system.actorOf(Coordinator.props(), "coord1");
        final ActorRef coord2 = system.actorOf(Coordinator.props(), "coord2");

        final ActorRef client1 = system.actorOf(TxnClient.props(1), "client1");
        final ActorRef client2 = system.actorOf(TxnClient.props(2), "client2");

        List<ActorRef> coordinatorList = List.of(coord1, coord2);
        ClientStartMsg clientMsg = new ClientStartMsg(coordinatorList, 19);
        client1.tell(clientMsg, ActorRef.noSender());
        client2.tell(clientMsg, ActorRef.noSender());

        final ActorRef server0 = system.actorOf(Server.props(0, 9), "server0");
        final ActorRef server1 = system.actorOf(Server.props(10, 19), "server1");

        server0.tell(new JoinMessage(10, 19), server1);
        server1.tell(new JoinMessage(0, 9), server0);

        coord1.tell(new JoinMessage(0, 9), server0);
        coord1.tell(new JoinMessage(10, 19), server1);
        coord2.tell(new JoinMessage(0, 9), server0);
        coord2.tell(new JoinMessage(10, 19), server1);


        server0.tell(new StartMessage(), ActorRef.noSender());
        server1.tell(new StartMessage(), ActorRef.noSender());
        coord1.tell(new StartMessage(), ActorRef.noSender());
        coord2.tell(new StartMessage(), ActorRef.noSender());

        Thread.sleep(200);

        system.terminate();
    }
}
