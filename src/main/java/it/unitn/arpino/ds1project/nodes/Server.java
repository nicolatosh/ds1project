package it.unitn.arpino.ds1project.nodes;

import akka.actor.Props;
import it.unitn.arpino.ds1project.datastore.Database;
import it.unitn.arpino.ds1project.datastore.Workspace;
import it.unitn.arpino.ds1project.transaction.Txn;
import it.unitn.arpino.ds1project.twopc.ServerFSM;

public class Server extends AbstractNode {
    private final Database database;

    protected Txn txn;
    protected Workspace workspace;
    protected ServerFSM twoPcFsm;

    public Server(int id) {
        super(id);
        database = new Database(id);
    }

    public static Props props(int serverId) {
        return Props.create(Server.class, () -> new Server(serverId));
    }

    @Override
    public Receive createReceive() {
        return super.createReceive();
    }

    private void initState() {
        workspace = new Workspace();
        twoPcFsm = new ServerFSM();
    }
}
