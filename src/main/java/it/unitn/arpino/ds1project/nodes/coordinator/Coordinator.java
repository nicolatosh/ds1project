package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.communication.Multicast;
import it.unitn.arpino.ds1project.messages.Transactional;
import it.unitn.arpino.ds1project.messages.client.ReadResultMsg;
import it.unitn.arpino.ds1project.messages.client.TxnAcceptMsg;
import it.unitn.arpino.ds1project.messages.client.TxnResultMsg;
import it.unitn.arpino.ds1project.messages.coordinator.*;
import it.unitn.arpino.ds1project.messages.server.*;
import it.unitn.arpino.ds1project.nodes.AbstractNode;
import it.unitn.arpino.ds1project.nodes.STATUS;
import it.unitn.arpino.ds1project.nodes.context.ContextManager;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class Coordinator extends AbstractNode {
    private final Dispatcher dispatcher;

    ContextManager<CoordinatorRequestContext> contextManager;

    public Coordinator() {
        status = STATUS.ALIVE;
        dispatcher = new Dispatcher();
        contextManager = new ContextManager<>();
    }

    public static Props props() {
        return Props.create(Coordinator.class, Coordinator::new);
    }


    @Override
    public void aroundPreStart() {
        super.aroundPreStart();
        getContext().setReceiveTimeout(Duration.ofSeconds(10000));
    }

    @Override
    public Receive createReceive() {
        return new ReceiveBuilder()
                .match(CoordinatorStartMsg.class, this::onCoordinatorStartMsg)
                .match(TxnBeginMsg.class, this::onTxnBeginMsg)
                .match(TxnEndMsg.class, this::onTxnEndMsg)
                .match(ReadMsg.class, this::onReadMsg)
                .match(ReadResult.class, this::onReadResult)
                .match(WriteMsg.class, this::onWriteMsg)
                .match(VoteResponse.class, this::onVoteResponse)
                .match(ReadResult.class, this::onReadResult)
                .build();
    }

    private Optional<CoordinatorRequestContext> getRequestContext(Transactional msg) {
        return contextManager.contextOf(msg);
    }

    private CoordinatorRequestContext newContext() {
        CoordinatorRequestContext ctx = new CoordinatorRequestContext(UUID.randomUUID(), getSender());

        contextManager.setActive(ctx);
        return ctx;
    }

    private void onCoordinatorStartMsg(CoordinatorStartMsg msg) {
        msg.serverKeys.forEach((server, keys) -> keys.forEach(key -> dispatcher.map(server, key)));
    }

    private void onTxnBeginMsg(TxnBeginMsg msg) {
        CoordinatorRequestContext ctx = this.newContext();

        TxnAcceptMsg response = new TxnAcceptMsg(ctx.uuid);
        getSender().tell(response, getSelf());
    }

    /**
     * Effectively starts the Two-phase commit (2PC) protocol
     */
    private void onTxnEndMsg(TxnEndMsg msg) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(msg);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        List<ActorRef> receivers = new ArrayList<>(ctx.get().participants);

        Multicast multicast = new Multicast()
                .setSender(getSelf())
                .addReceivers(receivers)
                .shuffle();

        if (msg.commit) {
            VoteRequest req = new VoteRequest(msg.uuid());
            multicast.setMessage(req);
        } else {
            AbortRequest req = new AbortRequest(msg.uuid());
            multicast.setMessage(req);
        }

        multicast.multicast();
    }

    private void onVoteResponse(VoteResponse resp) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(resp);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        switch (resp.vote) {
            case YES:
                ctx.get().yesVoters.add(getSender());

                if (ctx.get().allVotedYes()) {
                    ctx.get().protocolState = CoordinatorRequestContext.TwoPhaseCommitFSM.GLOBAL_COMMIT;

                    // multicast GLOBAL_COMMIT to all participants
                    FinalDecision decision = new FinalDecision(resp.uuid(), Decision.GLOBAL_COMMIT);
                    ctx.get().participants.forEach(server -> getContext().system().scheduler().scheduleOnce(
                            Duration.ofSeconds(1), server, decision, getContext().dispatcher(), getSelf()));

                    // tell the client the result of the transaction
                    TxnResultMsg result = new TxnResultMsg(resp.uuid(), true);
                    ctx.get().client.tell(result, getSelf());

                    // the transaction is completed: subsequent requests will begin a new transaction
                    contextManager.setCompleted(ctx.get());
                }
                break;

            case NO:
                // write GLOBAL_ABORT to local log
                ctx.get().protocolState = CoordinatorRequestContext.TwoPhaseCommitFSM.GLOBAL_ABORT;


                // multicast GLOBAL_ABORT to all participants
                FinalDecision decision = new FinalDecision(resp.uuid(), Decision.GLOBAL_ABORT);
                ctx.get().participants.forEach(server -> getContext().system().scheduler().scheduleOnce(
                        Duration.ofSeconds(1), server, decision, getContext().dispatcher(), getSelf()));

                // tell the client the result of the transaction
                TxnResultMsg result = new TxnResultMsg(resp.uuid(), false);
                ctx.get().client.tell(result, getSelf());

                // the transaction is completed: subsequent requests will begin a new transaction
                contextManager.setCompleted(ctx.get());

                break;
        }
    }

    private void onReadMsg(ReadMsg msg) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(msg);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        ReadRequest req = new ReadRequest(msg.uuid(), msg.key);

        ActorRef server = dispatcher.byKey(msg.key);
        server.tell(req, getSelf());

        ctx.get().participants.add(server);
        log.info("Request forwarded to server " + server.path().name() + ". Context:\n" + ctx.get());
    }

    private void onReadResult(ReadResult msg) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(msg);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        ReadResultMsg result = new ReadResultMsg(msg.uuid(), msg.key, msg.value);

        ctx.get().client.tell(result, getSelf());

    }

    private void onWriteMsg(WriteMsg msg) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(msg);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        WriteRequest req = new WriteRequest(msg.uuid(), msg.key, msg.key);

        ActorRef server = dispatcher.byKey(msg.key);
        server.tell(req, getSelf());

        ctx.get().participants.add(server);
        log.info("Request forwarded to server " + server.path().name() + ". Context:\n" + ctx.get());
    }
}
