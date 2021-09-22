package it.unitn.arpino.ds1project;

import akka.actor.ActorSystem;

public class Main {
    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create("ds1project");
        System.out.println("Hello, world!");
        system.terminate();
    }
}
