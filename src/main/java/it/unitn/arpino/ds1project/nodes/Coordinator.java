package it.unitn.arpino.ds1project.nodes;

import akka.actor.AbstractActor;
import akka.actor.Props;
import it.unitn.arpino.ds1project.transaction.TransactionId;

import java.util.ArrayList;
import java.util.List;

public class Coordinator extends AbstractActor {
    private final int coordinatorId;
    private final List<TransactionId> transactionIds;

    public Coordinator(int coordinatorId) {
        this.coordinatorId = coordinatorId;
        this.transactionIds = new ArrayList<>();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(TxnClient.TxnBeginMsg.class, this::onTxnBeginMsg)
                .build();
    }

    public static Props props(int coordinatorId) {
        return Props.create(Coordinator.class, () -> new Coordinator(coordinatorId));
    }

    private void onTxnBeginMsg(TxnClient.TxnBeginMsg msg) {
        TransactionId transactionId = new TransactionId(msg.clientId, this.coordinatorId);
        this.transactionIds.add(transactionId);

        getSender().tell(new TxnClient.TxnAcceptMsg(), getSelf());
    }
}
