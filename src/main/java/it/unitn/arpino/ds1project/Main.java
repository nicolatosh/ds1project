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
    public static void main(String[] args) {
        var system = ActorSystem.create("ds1project");

        // setup servers, coordinators and clients

        var server0 = system.actorOf(Server.props(0, 9), "server0");
        var server1 = system.actorOf(Server.props(10, 19), "server1");
        server0.tell(new JoinMessage(10, 19), server1);
        server1.tell(new JoinMessage(0, 9), server0);

        var coord0 = system.actorOf(Coordinator.props(), "coord0");
        var coord1 = system.actorOf(Coordinator.props(), "coord1");
        coord0.tell(new JoinMessage(0, 9), server0);
        coord0.tell(new JoinMessage(10, 19), server1);
        coord1.tell(new JoinMessage(0, 9), server0);
        coord1.tell(new JoinMessage(10, 19), server1);

        var start = new StartMessage();
        server0.tell(start, ActorRef.noSender());
        server1.tell(start, ActorRef.noSender());
        coord0.tell(start, ActorRef.noSender());
        coord1.tell(start, ActorRef.noSender());

        var client0 = system.actorOf(TxnClient.props(1), "client0");
        var client1 = system.actorOf(TxnClient.props(2), "client1");

        var clientStart = new ClientStartMsg(List.of(coord0, coord1), 19);
        client0.tell(clientStart, ActorRef.noSender());
        client1.tell(clientStart, ActorRef.noSender());
    }
}
