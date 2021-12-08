package it.unitn.arpino.ds1project.simulation;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.arpino.ds1project.messages.StartMessage;

import java.util.List;

public class Simulation {
    private final ActorSystem system;
    private final List<ActorRef> clients;
    private final List<ActorRef> coordinators;
    private final List<ActorRef> servers;

    // can be called by SimulationBuilder only
    protected Simulation(ActorSystem system, List<ActorRef> clients, List<ActorRef> coordinators, List<ActorRef> servers) {
        this.system = system;
        this.clients = clients;
        this.coordinators = coordinators;
        this.servers = servers;
    }

    public static SimulationBuilder builder() {
        return new SimulationBuilder();
    }

    public void start() {
        var start = new StartMessage();
        clients.forEach(client -> client.tell(start, ActorRef.noSender()));
    }

    public ActorSystem getSystem() {
        return system;
    }

    public List<ActorRef> getClients() {
        return clients;
    }

    public List<ActorRef> getCoordinators() {
        return coordinators;
    }

    public List<ActorRef> getServers() {
        return servers;
    }
}
