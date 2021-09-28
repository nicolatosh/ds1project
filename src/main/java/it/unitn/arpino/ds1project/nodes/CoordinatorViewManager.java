package it.unitn.arpino.ds1project.nodes;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.transaction.Txn;
import it.unitn.arpino.ds1project.twopc.CoordinatorFSM;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CoordinatorViewManager extends AbstractViewManager<Coordinator> {
    private final Set<Txn> txn;
    private final Map<Txn, CoordinatorFSM> twoPcFSM;
    private final Map<Txn, Set<ActorRef>> yesVoters;

    public CoordinatorViewManager(Coordinator node) {
        super(node);
        txn = new HashSet<>();
        twoPcFSM = new HashMap<>();
        yesVoters = new HashMap<>();
    }

    @Override
    protected void changeView(Txn txn) {
        node.txn = txn;
        node.twoPcFSM = twoPcFSM.get(txn);
        node.yesVoters = yesVoters.get(txn);
    }

    @Override
    protected void sync() {
        Txn txn = node.txn;

        this.txn.add(node.txn);
        this.twoPcFSM.putIfAbsent(txn, node.twoPcFSM);
        this.yesVoters.putIfAbsent(txn, node.yesVoters);
    }
}
