package it.unitn.arpino.ds1project.nodes;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.datastore.Database;
import it.unitn.arpino.ds1project.datastore.Workspace;
import it.unitn.arpino.ds1project.transaction.Txn;
import it.unitn.arpino.ds1project.transaction.messages.ReadMsgCoordinator;
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
        Receive receive = new ReceiveBuilder()
                .match(ReadMsgCoordinator.class, this::onReadMsgCoordinator)
                .build();
        return super.createReceive().orElse(receive);
    }

    private void initState() {
        workspace = new Workspace();
        twoPcFsm = new ServerFSM();
    }

    private void onReadMsgCoordinator(ReadMsgCoordinator msg) {

    }
}
