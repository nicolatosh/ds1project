package it.unitn.arpino.ds1project.nodes;

import akka.actor.AbstractActor;
import akka.actor.Props;
import it.unitn.arpino.ds1project.datastore.Database;

public class Server extends AbstractActor {
    private final int serverId;
    private final Database database;

    public Server(int serverId) {
        this.serverId = serverId;
        this.database = new Database(serverId);
    }

    public static Props props(int serverId) {
        return Props.create(Server.class, () -> new Server(serverId));
    }

    @Override
    public Receive createReceive() {
        return null;
    }
}
