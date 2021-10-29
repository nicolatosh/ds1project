package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.messages.Transactional;
import it.unitn.arpino.ds1project.messages.client.ReadResultMsg;
import it.unitn.arpino.ds1project.messages.client.TxnAcceptMsg;
import it.unitn.arpino.ds1project.messages.client.TxnResultMsg;
import it.unitn.arpino.ds1project.messages.coordinator.*;
import it.unitn.arpino.ds1project.messages.server.*;
import it.unitn.arpino.ds1project.nodes.AbstractNode;
import it.unitn.arpino.ds1project.nodes.context.ContextManager;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.IntStream;

public class Coordinator extends AbstractNode {
    private final Dispatcher dispatcher;

    ContextManager<CoordinatorRequestContext> contextManager;

    public Coordinator() {
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
        msg.serverInfos.forEach(info -> IntStream.rangeClosed(info.lowerKey, info.upperKey)
                .forEach(key -> dispatcher.map(info.server, key)));
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

        if (msg.commit) {
            ctx.get().setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.WAIT);

            VoteRequest req = new VoteRequest(msg.uuid());
            ctx.get().getParticipants().forEach(participant -> participant.tell(req, getSelf()));
        } else {
            ctx.get().setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT);

            AbortRequest req = new AbortRequest(msg.uuid());
            ctx.get().getParticipants().forEach(participant -> participant.tell(req, getSelf()));

            contextManager.setCompleted(ctx.get());
        }
    }

    private void onVoteResponse(VoteResponse resp) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(resp);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        switch (resp.vote) {
            case YES:
                ctx.get().addYesVoter(getSender());

                if (ctx.get().allVotedYes()) {
                    ctx.get().setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.COMMIT);

                    // multicast GLOBAL_COMMIT to all participants
                    FinalDecision decision = new FinalDecision(resp.uuid(), FinalDecision.Decision.GLOBAL_COMMIT);
                    ctx.get().getParticipants().forEach(server -> getContext().system().scheduler().scheduleOnce(
                            Duration.ofSeconds(1), server, decision, getContext().dispatcher(), getSelf()));

                    // tell the client the result of the transaction
                    TxnResultMsg result = new TxnResultMsg(resp.uuid(), true);
                    ctx.get().getClient().tell(result, getSelf());

                    // the transaction is completed: subsequent requests will begin a new transaction
                    contextManager.setCompleted(ctx.get());
                }
                break;

            case NO:
                // write GLOBAL_ABORT to local log
                ctx.get().setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT);


                // multicast GLOBAL_ABORT to all participants
                FinalDecision decision = new FinalDecision(resp.uuid(), FinalDecision.Decision.GLOBAL_ABORT);
                ctx.get().getParticipants().forEach(server -> getContext().system().scheduler().scheduleOnce(
                        Duration.ofSeconds(1), server, decision, getContext().dispatcher(), getSelf()));

                // tell the client the result of the transaction
                TxnResultMsg result = new TxnResultMsg(resp.uuid(), false);
                ctx.get().getClient().tell(result, getSelf());

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

        ctx.get().addParticipant(server);
        log.info("Request forwarded to server " + server.path().name() + ". Context:\n" + ctx.get());
    }

    private void onReadResult(ReadResult msg) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(msg);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        ReadResultMsg result = new ReadResultMsg(msg.uuid(), msg.key, msg.value);

        ctx.get().getClient().tell(result, getSelf());
    }

    private void onWriteMsg(WriteMsg msg) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(msg);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        WriteRequest req = new WriteRequest(msg.uuid(), msg.key, msg.value);

        ActorRef server = dispatcher.byKey(msg.key);
        server.tell(req, getSelf());

        ctx.get().addParticipant(server);
        log.info("Request forwarded to server " + server.path().name() + ". Context:\n" + ctx.get());
    }
}
