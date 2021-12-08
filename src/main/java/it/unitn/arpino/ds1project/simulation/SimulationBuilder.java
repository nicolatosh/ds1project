package it.unitn.arpino.ds1project.simulation;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.arpino.ds1project.messages.JoinMessage;
import it.unitn.arpino.ds1project.messages.StartMessage;
import it.unitn.arpino.ds1project.messages.client.CoordinatorList;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SimulationBuilder {
    private int nClients;
    private int nCoordinators;
    private int nServers;

    // can be called by the Simulation only
    protected SimulationBuilder() {
    }

    public SimulationBuilder ofClients(int nClients) throws IllegalArgumentException {
        if (nClients < 0) {
            throw new IllegalArgumentException("nClients must be >= 0");
        }
        this.nClients = nClients;
        return this;
    }

    public SimulationBuilder ofCoordinators(int nCoordinators) throws IllegalArgumentException {
        if (nCoordinators < 0) {
            throw new IllegalArgumentException("nCoordinators must be >= 0");
        }
        this.nCoordinators = nCoordinators;
        return this;
    }

    public SimulationBuilder ofServers(int nServers) throws IllegalArgumentException {
        if (nServers < 0) {
            throw new IllegalArgumentException("nServers must be >= 0");
        }
        this.nServers = nServers;
        return this;
    }

    public Simulation build() {
        var system = ActorSystem.create("ds1project");

        var clients = IntStream.rangeClosed(0, nClients)
                .mapToObj(i -> system.actorOf(TxnClient.props(), "client" + i))
                .collect(Collectors.toList());

        var coordinators = IntStream.rangeClosed(0, nCoordinators)
                .mapToObj(i -> system.actorOf(Coordinator.props(), "coord" + i))
                .collect(Collectors.toList());

        var servers = IntStream.rangeClosed(0, nServers)
                .mapToObj(i -> system.actorOf(Server.props(i * 10, i * 10 + 9), "server" + i))
                .collect(Collectors.toList());

        for (int i = 0; i < servers.size(); i++) {
            var server = servers.get(i);

            var join = new JoinMessage(i * 10, i * 10 + 9);

            servers.stream()
                    .filter(other -> other != server)
                    .forEach(other -> other.tell(join, server));

            coordinators.forEach(coordinator -> coordinator.tell(join, server));
        }

        var list = new CoordinatorList(coordinators, ((nServers - 1) * 10) + 9);
        clients.forEach(client -> client.tell(list, ActorRef.noSender()));

        var start = new StartMessage();
        servers.forEach(server -> server.tell(start, ActorRef.noSender()));
        coordinators.forEach(coordinator -> coordinator.tell(start, ActorRef.noSender()));

        return new Simulation(system, clients, coordinators, servers);
    }
}
