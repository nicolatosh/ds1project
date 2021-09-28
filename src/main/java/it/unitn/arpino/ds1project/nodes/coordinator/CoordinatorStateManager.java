package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.nodes.AbstractStateManager;
import it.unitn.arpino.ds1project.twopc.CoordinatorFSM;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CoordinatorStateManager extends AbstractStateManager<Coordinator> {
    private final Set<ActorRef> ongoingTxns;

    private final Map<ActorRef, CoordinatorFSM> twoPcFSMStore;
    private final Map<ActorRef, Set<ActorRef>> yesVotersStore;

    public CoordinatorStateManager(Coordinator coordinator) {
        super(coordinator);

        ongoingTxns = new HashSet<>();

        twoPcFSMStore = new HashMap<>();
        yesVotersStore = new HashMap<>();
    }

    @Override
    protected void before() {
        ActorRef actorRef = node.getSender();

        node.twoPcFSM = new CoordinatorFSM();
        node.yesVoters = new HashSet<>();
    }

    @Override
    protected void after() {
        ActorRef actorRef = node.getSender();

        ongoingTxns.add(actorRef);

        node.twoPcFSM = twoPcFSMStore.putIfAbsent(actorRef, new CoordinatorFSM());
        node.yesVoters = yesVotersStore.putIfAbsent(actorRef, new HashSet<>());
    }
}
