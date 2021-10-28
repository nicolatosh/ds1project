package it.unitn.arpino.ds1project;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.arpino.ds1project.messages.client.ClientStartMsg;
import it.unitn.arpino.ds1project.messages.coordinator.CoordinatorStartMsg;
import it.unitn.arpino.ds1project.messages.server.ServerStartMsg;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Main {
    public static void main(String[] args) {
        final ActorSystem system = ActorSystem.create("ds1project");

        final ActorRef client1 = system.actorOf(TxnClient.props(1), "client1");
        final ActorRef client2 = system.actorOf(TxnClient.props(2), "client2");

        final ActorRef coord1 = system.actorOf(Coordinator.props(), "coord1");
        final ActorRef coord2 = system.actorOf(Coordinator.props(), "coord2");

        final ActorRef server0 = system.actorOf(Server.props(0, 9), "server0");
        final ActorRef server1 = system.actorOf(Server.props(10, 19), "server1");

        CoordinatorStartMsg coordMsg = new CoordinatorStartMsg(List.of(
                new CoordinatorStartMsg.ServerInfo(server0, 0, 9),
                new CoordinatorStartMsg.ServerInfo(server1, 1, 19)
        ));
        coord1.tell(coordMsg, ActorRef.noSender());
        coord2.tell(coordMsg, ActorRef.noSender());

        ClientStartMsg clientMsg = new ClientStartMsg(List.of(coord1, coord2), IntStream.rangeClosed(0, 19).boxed().collect(Collectors.toList()));
        client1.tell(clientMsg, ActorRef.noSender());
        client2.tell(clientMsg, ActorRef.noSender());


        server0.tell(new ServerStartMsg(List.of(server1)), ActorRef.noSender());
        server1.tell(new ServerStartMsg(List.of(server0)), ActorRef.noSender());

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        system.terminate();
    }
}
