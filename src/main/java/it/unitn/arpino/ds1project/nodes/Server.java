package it.unitn.arpino.ds1project.nodes;

import akka.actor.AbstractActor;
import akka.actor.Props;
import it.unitn.arpino.ds1project.datastore.Database;
import it.unitn.arpino.ds1project.datastore.Workspace;

import java.util.HashMap;
import java.util.Map;

public class Server extends AbstractActor {
    private final int serverId;
    private final Database database;
    private final Map<Integer, Workspace> workspaces;

    public Server(int serverId) {
        this.serverId = serverId;
        this.database = new Database(serverId);
        this.workspaces = new HashMap<>();
    }

    public static Props props(int serverId) {
        return Props.create(Server.class, () -> new Server(serverId));
    }

    @Override
    public Receive createReceive() {
        return null;
    }

    // Todo: transactionId type
    private void createWorkspace(int transactionId) {
        this.workspaces.put(transactionId, new Workspace(transactionId));
    }
}
