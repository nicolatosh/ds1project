package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.communication.Multicast;
import it.unitn.arpino.ds1project.messages.client.TxnAcceptMsg;
import it.unitn.arpino.ds1project.messages.control.StartMessage;
import it.unitn.arpino.ds1project.messages.coordinator.ReadMsg;
import it.unitn.arpino.ds1project.messages.coordinator.TxnBeginMsg;
import it.unitn.arpino.ds1project.messages.coordinator.TxnEndMsg;
import it.unitn.arpino.ds1project.messages.server.ReadRequest;
import it.unitn.arpino.ds1project.twopc.CoordinatorFSM;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Coordinator extends AbstractActor {
    Set<ActorRef> servers;
    Map<Integer, ActorRef> keys;

    protected CoordinatorFSM twoPcFSM;
    protected Set<ActorRef> yesVoters;

    public Coordinator() {
        servers = new HashSet<>();
        keys = new HashMap<>();
    }

    public static Props props() {
        return Props.create(Coordinator.class, Coordinator::new);
    }

    @Override
    public Receive createReceive() {
        return new ReceiveBuilder()
                .match(StartMessage.class, this::onStartMsg)
                .match(TxnBeginMsg.class, this::onTxnBeginMsg)
                .match(ReadMsg.class, this::onReadMsg)
                .build();
    }

    private void onStartMsg(StartMessage msg) {
        servers.addAll(msg.servers);
    }

    private void onTxnBeginMsg(TxnBeginMsg msg) {
        twoPcFSM = new CoordinatorFSM();
        yesVoters = new HashSet<>();

        TxnAcceptMsg response = new TxnAcceptMsg();
        getSender().tell(response, getSelf());
    }

    /**
     * Read request by client to read data by key
     *
     * @param msg
     */
    private void onReadMsg(ReadMsg msg) {
        ReadRequest readRequest = new ReadRequest(getSender(), msg.key);

        ActorRef server = keys.get(msg.key);
        server.tell(readRequest, getSelf());
    }

    /**
     * Effectively starts the 2PC (Two-phase commit) protocol
     *
     * @param msg
     */
    private void onTxnEndMsg(TxnEndMsg msg) {
        new Multicast(getSelf(), servers, msg).multicast();

        twoPcFSM.setState(CoordinatorFSM.STATE.WAIT);
    }
}
