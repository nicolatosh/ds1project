package it.unitn.arpino.ds1project;

import it.unitn.arpino.ds1project.simulation.Simulation;

public class Main {
    public static void main(String[] args) {
        var simulation = Simulation.builder()
                .ofClients(1)
                .ofCoordinators(1)
                .ofServers(2)
                .build();
        System.out.println("Starting the simulation");
        simulation.start();
    }
}
