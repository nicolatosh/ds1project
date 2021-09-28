package it.unitn.arpino.ds1project.nodes;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.transaction.Txn;
import it.unitn.arpino.ds1project.transaction.messages.TxnAcceptMsg;
import it.unitn.arpino.ds1project.transaction.messages.TxnBeginMsg;
import it.unitn.arpino.ds1project.transaction.messages.TxnEndMsg;
import it.unitn.arpino.ds1project.twopc.CoordinatorFSM;
import it.unitn.arpino.ds1project.twopc.messages.VoteRequest;

import java.util.HashSet;
import java.util.Set;

public class Coordinator extends AbstractNode {
    protected Txn txn;
    protected CoordinatorFSM twoPcFSM;
    protected Set<ActorRef> yesVoters;

    public Coordinator(int id) {
        super(id);
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
        txn = new Txn(msg.clientId);
        twoPcFSM = new CoordinatorFSM();
        yesVoters = new HashSet<>();

        getSender().tell(new TxnAcceptMsg(), getSelf());
    }

    /**
     * Effectively starts the 2PC (Two-phase commit) protocol
     *
     * @param msg
     */
    private void onTxnEndMsg(TxnEndMsg msg) {
        int clientId = msg.clientId;

        multicast(new VoteRequest(txn.uuid));

        twoPcFSM.setState(CoordinatorFSM.STATE.WAIT);
    }
}
