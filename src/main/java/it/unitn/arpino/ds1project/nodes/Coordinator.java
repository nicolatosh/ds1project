package it.unitn.arpino.ds1project.nodes;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.transaction.Txn;
import it.unitn.arpino.ds1project.transaction.messages.*;
import it.unitn.arpino.ds1project.twopc.CoordinatorFSM;
import it.unitn.arpino.ds1project.twopc.messages.VoteRequest;

import java.util.HashSet;
import java.util.Optional;
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
                .match(ReadMsg.class, this::onReadMsg)
                .build();
        return super.createReceive()
                .orElse(receive);
    }

    /**
     * Read request by client to read data by key
     *
     * @param msg
     */
    private void onReadMsg(ReadMsg msg) {
        Optional<ActorRef> serverRef = getServerByKey(msg.key);
        serverRef.ifPresent(actorRef -> {
            ReadMsgCoordinator read_req = new ReadMsgCoordinator(txn, id, msg.key);
            actorRef.tell(read_req, getSelf());
        });
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

        multicast(new VoteRequest(txn));

        twoPcFSM.setState(CoordinatorFSM.STATE.WAIT);
    }

    /**
     * Allows to get the server ActorRef by key
     *
     * @param key
     * @return
     */
    private Optional<ActorRef> getServerByKey(int key) {
        return group.stream()
                .filter(pair -> pair.b == key)
                .map(pair -> pair.a)
                .findFirst();
    }
}
