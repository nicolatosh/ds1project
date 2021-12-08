package it.unitn.arpino.ds1project.simulation;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SimulationBuilder {

    private List<TxnClient> clients;
    private List<ActorRef> coordinators;
    private List<ActorRef> servers;

    private SimulationBuilder() {
    }

    /**
     * Constructor with 0 args
     *
     * @return SimulationBuilder
     */
    public static SimulationBuilder builder() {
        return new SimulationBuilder();
    }

    /**
     * Constructor with configurable amount of actors.
     * Generate simulation given number of actors involved
     *
     * @param clients_n      Number of TxnClients@ {@link TxnClient}
     * @param coordinators_n Number of Coordinators {@link Coordinator}
     * @param servers_n      Number of TxnClients {@link Server}
     * @return SimulationBuilder
     */
    public static SimulationBuilder builder(int clients_n, int coordinators_n, int servers_n) {

        var clients = Stream.generate(TxnClient::new).limit(clients_n).collect(Collectors.toList());
        var coordinators = Stream.generate(Coordinator::new).limit(coordinators_n).map(AbstractActor::getSelf).collect(Collectors.toList());
        Set<ActorRef> servers = new HashSet<>();

        for (int i = 0; i < servers_n; i++) {
            servers.add(new Server(i * 10, i * 10 + 9).getSelf());
        }

        // Servers should know each other

        return builder()
                .ofClients(clients)
                .ofCoordinators(coordinators)
                .ofServers(servers);

    }

    public SimulationBuilder ofClients(Collection<TxnClient> clients) {
        this.clients.addAll(clients);
        return this;
    }

    public SimulationBuilder ofCoordinators(Collection<ActorRef> coordinators) {
        this.coordinators.addAll(coordinators);
        return this;
    }

    public SimulationBuilder ofServers(Collection<ActorRef> servers) {
        this.servers.addAll(servers);
        return this;
    }

    public List<TxnClient> getClients() {
        return clients;
    }

    public List<ActorRef> getCoordinators() {
        return coordinators;
    }

    public List<ActorRef> getServers() {
        return servers;
    }
}
