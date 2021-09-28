package it.unitn.arpino.ds1project.nodes;

import akka.actor.Props;
import it.unitn.arpino.ds1project.datastore.Database;
import it.unitn.arpino.ds1project.datastore.Workspace;
import it.unitn.arpino.ds1project.twopc.ServerFSM;

import java.util.HashMap;
import java.util.Map;

public class Server extends AbstractNode {
    private final Database database;
    private final Map<Integer, Workspace> workspaces;
    private final Map<Integer, ServerFSM> twoPcFsm;

    public Server(int id) {
        super(id);
        database = new Database(id);
        workspaces = new HashMap<>();
        twoPcFsm = new HashMap<>();
    }

    public static Props props(int serverId) {
        return Props.create(Server.class, () -> new Server(serverId));
    }

    @Override
    public Receive createReceive() {
        return super.createReceive();
    }

    // Todo: transactionId type
    private void createWorkspace(int transactionId) {
        workspaces.put(transactionId, new Workspace(transactionId));
    }
}
