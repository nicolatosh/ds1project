package it.unitn.arpino.ds1project.nodes;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.transaction.TransactionId;

import java.util.ArrayList;
import java.util.List;

public class Coordinator extends AbstractNode {
    private final List<TransactionId> transactionIds;

    public Coordinator(int id) {
        super(id);
        this.transactionIds = new ArrayList<>();
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
        TransactionId transactionId = new TransactionId(msg.clientId, this.id);
        this.transactionIds.add(transactionId);

        getSender().tell(new TxnClient.TxnAcceptMsg(), getSelf());
    }
}
