package it.unitn.arpino.ds1project;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.arpino.ds1project.messages.ServerInfo;
import it.unitn.arpino.ds1project.messages.client.ClientStartMsg;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.List;

public class Main {
    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("ds1project");

        // setup servers and coordinators

        final ActorRef server0 = system.actorOf(Server.props(0, 9), "server0");
        final ActorRef server1 = system.actorOf(Server.props(10, 19), "server1");

        final ActorRef coord1 = system.actorOf(Coordinator.props(), "coord1");
        final ActorRef coord2 = system.actorOf(Coordinator.props(), "coord2");

        server0.tell(new ServerInfo(server1, 10, 19), ActorRef.noSender());
        server1.tell(new ServerInfo(server0, 0, 9), ActorRef.noSender());

        coord1.tell(new ServerInfo(server0, 0, 9), ActorRef.noSender());
        coord1.tell(new ServerInfo(server1, 10, 19), ActorRef.noSender());
        coord2.tell(new ServerInfo(server0, 0, 9), ActorRef.noSender());
        coord2.tell(new ServerInfo(server1, 10, 19), ActorRef.noSender());

        // setup client

        final ActorRef client1 = system.actorOf(TxnClient.props(1), "client1");
        final ActorRef client2 = system.actorOf(TxnClient.props(2), "client2");

        List<ActorRef> coordinatorList = List.of(coord1, coord2);
        ClientStartMsg clientMsg = new ClientStartMsg(coordinatorList, 19);

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        client1.tell(clientMsg, ActorRef.noSender());
        client2.tell(clientMsg, ActorRef.noSender());

        system.terminate();
    }
}
