package it.unitn.arpino.ds1project;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.arpino.ds1project.messages.client.ClientStartMsg;
import it.unitn.arpino.ds1project.messages.coordinator.CoordinatorStartMsg;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        // The list of keys of the various data items

        List<Integer> keyList1 = List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        List<Integer> keyList2 = List.of(10, 11, 12, 13, 14, 15, 16, 17, 18, 19);
        List<Integer> keys = new ArrayList<>(keyList1.size() + keyList2.size());
        keys.addAll(keyList1);
        keys.addAll(keyList2);

        // Create the nodes

        final ActorSystem system = ActorSystem.create("ds1project");

        final ActorRef client1 = system.actorOf(TxnClient.props(1), "client1");
        final ActorRef client2 = system.actorOf(TxnClient.props(2), "client2");

        final ActorRef coord1 = system.actorOf(Coordinator.props(), "coord1");
        final ActorRef coord2 = system.actorOf(Coordinator.props(), "coord2");

        final ActorRef server1 = system.actorOf(Server.props(), "server1");
        final ActorRef server2 = system.actorOf(Server.props(), "server2");


        // Provide the clients information about the context

        ClientStartMsg clientMsg = new ClientStartMsg(List.of(coord1, coord2), keys);
        client1.tell(clientMsg, ActorRef.noSender());
        client2.tell(clientMsg, ActorRef.noSender());

        // Provide the coordinators information about the context

        Map<ActorRef, List<Integer>> serverKeys = new HashMap<>();
        serverKeys.put(client1, keyList1);
        serverKeys.put(client2, keyList2);

        CoordinatorStartMsg coordMsg = new CoordinatorStartMsg(serverKeys);
        coord1.tell(coordMsg, ActorRef.noSender());
        coord2.tell(coordMsg, ActorRef.noSender());

        // Provide the servers information about the context

//        server1.tell(new ServerStartMsg(List.of(server2)), ActorRef.noSender());
//        server2.tell(new ServerStartMsg(List.of(server1)), ActorRef.noSender());

        system.terminate();
    }
}
