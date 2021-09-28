package it.unitn.arpino.ds1project.nodes;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.transaction.messages.TxnAcceptMsg;
import it.unitn.arpino.ds1project.transaction.messages.TxnBeginMsg;
import it.unitn.arpino.ds1project.transaction.messages.TxnEndMsg;
import it.unitn.arpino.ds1project.twopc.CoordinatorFSM;
import it.unitn.arpino.ds1project.twopc.messages.VoteRequest;

import java.util.*;

public class Coordinator extends AbstractNode {
    private final List<Integer> clientIds;
    private final Map<Integer, CoordinatorFSM> twoPcFSM;
    private final Map<Integer, Set<ActorRef>> yesVoters;

    public Coordinator(int id) {
        super(id);
        clientIds = new ArrayList<>();
        twoPcFSM = new HashMap<>();
        yesVoters = new HashMap<>();
    }

    @Override
    public Receive createReceive() {
        Receive receive = new ReceiveBuilder()
                .match(TxnBeginMsg.class, this::onTxnBeginMsg)
                .build();
        return super.createReceive()
                .orElse(receive);
    }

    public static Props props(int id) {
        return Props.create(Coordinator.class, () -> new Coordinator(id));
    }

    private void onTxnBeginMsg(TxnBeginMsg msg) {
        int clientId = msg.clientId;

        clientIds.add(clientId);

        CoordinatorFSM fsm = new CoordinatorFSM();
        twoPcFSM.put(clientId, fsm);

        getSender().tell(new TxnAcceptMsg(), getSelf());
    }

    /**
     * Effectively starts the 2PC (Two-phase commit) protocol
     *
     * @param msg
     */
    private void onTxnEndMsg(TxnEndMsg msg) {
        int clientId = msg.clientId;

        multicast(new VoteRequest());

        CoordinatorFSM fsm = this.twoPcFSM.get(clientId);
        fsm.setState(CoordinatorFSM.STATE.WAIT);
    }
}
