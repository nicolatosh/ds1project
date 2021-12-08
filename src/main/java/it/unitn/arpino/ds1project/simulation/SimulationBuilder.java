package it.unitn.arpino.ds1project.simulation;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.arpino.ds1project.messages.JoinMessage;
import it.unitn.arpino.ds1project.nodes.client.TxnClient;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.Server;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SimulationBuilder {

    private List<ActorRef> clients = new ArrayList<>();
    private List<ActorRef> coordinators = new ArrayList<>();
    private List<ActorRef> servers = new ArrayList<>();

    public SimulationBuilder() {
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
    public static SimulationBuilder builder(ActorSystem system, int clients_n, int coordinators_n, int servers_n) {

        List<ActorRef> clients = new ArrayList<>();
        List<ActorRef> coordinators = new ArrayList<>();
        List<ActorRef> servers = new ArrayList<>();

        for (int i = 0; i < clients_n; i++) {
            clients.add(system.actorOf(TxnClient.props(), "client-" + i));
        }

        for (int i = 0; i < coordinators_n; i++) {
            coordinators.add(system.actorOf(Coordinator.props(), "coordinator-" + i));
        }

        for (int i = 0; i < servers_n; i++) {
            var server = system.actorOf(Server.props(i * 10, i * 10 + 9), "server-" + i);
            servers.add(server);

            // Telling coordinators about new server
            for (ActorRef coordinator : coordinators) {
                coordinator.tell(new JoinMessage(i * 10, i * 10 + 9), server);
            }

            // Servers must know each other
            for (int k = 0; k < servers.size() - 1; k++) {
                servers.get(k).tell(new JoinMessage(i * 10, i * 10 + 9), server);
                server.tell(new JoinMessage(k * 10, k * 10 + 9), servers.get(k));
            }
        }

        return builder()
                .ofClients(clients)
                .ofCoordinators(coordinators)
                .ofServers(servers);

    }

    public SimulationBuilder ofClients(Collection<ActorRef> clients) {
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
