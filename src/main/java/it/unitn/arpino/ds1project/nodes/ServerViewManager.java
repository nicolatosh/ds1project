package it.unitn.arpino.ds1project.nodes;

import it.unitn.arpino.ds1project.datastore.Workspace;
import it.unitn.arpino.ds1project.transaction.Txn;
import it.unitn.arpino.ds1project.twopc.ServerFSM;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ServerViewManager extends AbstractViewManager<Server> {
    private final Set<Txn> txn;
    private final Map<Txn, Workspace> workspace;
    private final Map<Txn, ServerFSM> twoPcFsm;

    public ServerViewManager(Server node) {
        super(node);
        txn = new HashSet<>();
        workspace = new HashMap<>();
        twoPcFsm = new HashMap<>();
    }

    @Override
    protected void changeView(Txn txn) {
        node.txn = txn;

        node.workspace = workspace.get(txn);
        node.twoPcFsm = twoPcFsm.get(txn);

    }

    @Override
    protected void sync() {
        Txn txn = node.txn;

        this.txn.add(node.txn);
        this.workspace.putIfAbsent(txn, node.workspace);
        this.twoPcFsm.putIfAbsent(txn, node.twoPcFsm);
    }
}
