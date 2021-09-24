package it.unitn.arpino.ds1project.nodes;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.japi.Pair;

import java.util.*;

public class Server extends AbstractActor {

    private final Integer serverId;
    private DataItem dataItem;

    /*-- Actor constructor ---------------------------------------------------- */

    public Server(int serverId) {
        this.serverId = serverId;
        this.dataItem = new DataItem(serverId);
    }

    static public Props props(int serverId) {
        return Props.create(Server.class, () -> new Server(serverId));
    }


    @Override
    public Receive createReceive() {
        return null;
    }
}
