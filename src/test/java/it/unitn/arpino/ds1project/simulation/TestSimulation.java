package it.unitn.arpino.ds1project.simulation;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.arpino.ds1project.messages.StartMessage;
import it.unitn.arpino.ds1project.messages.client.ClientStartMsg;
import org.junit.jupiter.api.Test;

public class TestSimulation {

    ActorSystem system;
    SimulationBuilder builder;

    @Test
    void startTest() throws InterruptedException {
        system = ActorSystem.create();

        this.builder = SimulationBuilder.builder(system, 1, 1, 2);
        var clients = this.builder.getClients();
        var coordinators = this.builder.getCoordinators();
        var servers = this.builder.getServers();


        var startMsg = new StartMessage();

        // Starting actors
        servers.forEach(s -> s.tell(startMsg, ActorRef.noSender()));
        coordinators.forEach(c -> c.tell(startMsg, ActorRef.noSender()));
        clients.forEach(c -> c.tell(new ClientStartMsg(coordinators, servers.size() * 10 + 9), ActorRef.noSender()));
        clients.forEach(c -> c.tell(startMsg, ActorRef.noSender()));

        Thread.sleep(4000);
    }
}
