package it.unitn.arpino.ds1project.nodes;

import akka.actor.ActorRef;
import it.unitn.arpino.ds1project.datastore.Workspace;
import it.unitn.arpino.ds1project.twopc.ServerFSM;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ServerStateManager extends AbstractStateManager<Server> {
    private final Set<ActorRef> ongoingTxns;

    private final Map<ActorRef, Workspace> workspaceStore;
    private final Map<ActorRef, ServerFSM> twoPcFSMStore;

    public ServerStateManager(Server node) {
        super(node);

        ongoingTxns = new HashSet<>();

        workspaceStore = new HashMap<>();
        twoPcFSMStore = new HashMap<>();
    }

    @Override
    protected void before() {
        ActorRef actorRef = node.getSender();

        node.workspace = new Workspace();
        node.twoPcFsm = new ServerFSM();
    }

    @Override
    protected void after() {
        ActorRef actorRef = node.getSender();

        ongoingTxns.add(actorRef);

        node.workspace = workspaceStore.putIfAbsent(actorRef, new Workspace());
        node.twoPcFsm = twoPcFSMStore.putIfAbsent(actorRef, new ServerFSM());
    }
}
