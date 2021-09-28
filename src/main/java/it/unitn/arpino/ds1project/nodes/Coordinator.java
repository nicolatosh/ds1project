package it.unitn.arpino.ds1project.nodes;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.twopc.CoordinatorFSM;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Coordinator extends AbstractNode {
    private final List<Integer> clientIds;
    private final Map<Integer, CoordinatorFSM> twoPcFSM;

    public Coordinator(int id) {
        super(id);
        clientIds = new ArrayList<>();
        twoPcFSM = new HashMap<>();
    }

    @Override
    public Receive createReceive() {
        Receive receive = new ReceiveBuilder()
                .match(TxnClient.TxnBeginMsg.class, this::onTxnBeginMsg)
                .build();
        return super.createReceive()
                .orElse(receive);
    }

    public static Props props(int id) {
        return Props.create(Coordinator.class, () -> new Coordinator(id));
    }

    private void onTxnBeginMsg(TxnClient.TxnBeginMsg msg) {
        int clientId = msg.clientId;

        clientIds.add(clientId);

        CoordinatorFSM fsm = new CoordinatorFSM();
        twoPcFSM.put(clientId, fsm);

        getSender().tell(new TxnClient.TxnAcceptMsg(), getSelf());
    }
}
