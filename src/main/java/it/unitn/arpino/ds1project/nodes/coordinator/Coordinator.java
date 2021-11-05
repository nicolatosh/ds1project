package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.messages.ServerInfo;
import it.unitn.arpino.ds1project.messages.TimeoutExpired;
import it.unitn.arpino.ds1project.messages.client.ReadResultMsg;
import it.unitn.arpino.ds1project.messages.client.TxnAcceptMsg;
import it.unitn.arpino.ds1project.messages.client.TxnResultMsg;
import it.unitn.arpino.ds1project.messages.coordinator.*;
import it.unitn.arpino.ds1project.messages.server.FinalDecision;
import it.unitn.arpino.ds1project.messages.server.ReadRequest;
import it.unitn.arpino.ds1project.messages.server.VoteRequest;
import it.unitn.arpino.ds1project.messages.server.WriteRequest;
import it.unitn.arpino.ds1project.nodes.DataStoreNode;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.IntStream;

public class Coordinator extends DataStoreNode<CoordinatorRequestContext> {
    private final Dispatcher dispatcher;

    public Coordinator() {
        dispatcher = new Dispatcher();
    }

    public Dispatcher getDispatcher() {
        return dispatcher;
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
                .match(ServerInfo.class, this::onServerInfo)
                .match(TxnBeginMsg.class, this::onTxnBeginMsg)
                .match(TxnEndMsg.class, this::onTxnEndMsg)
                .match(ReadMsg.class, this::onReadMsg)
                .match(ReadResult.class, this::onReadResult)
                .match(WriteMsg.class, this::onWriteMsg)
                .match(VoteResponse.class, this::onVoteResponse)
                .match(TimeoutExpired.class, this::onTimeoutExpired)
                .build();
    }

    private CoordinatorRequestContext newContext() {
        CoordinatorRequestContext ctx = new CoordinatorRequestContext(UUID.randomUUID(), getSender());

        addContext(ctx);
        return ctx;
    }

    private void onServerInfo(ServerInfo server) {
        IntStream.rangeClosed(server.lowerKey, server.upperKey)
                .forEach(key -> dispatcher.map(key, server.server));
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

            FinalDecision decision = new FinalDecision(ctx.get().uuid, FinalDecision.Decision.GLOBAL_ABORT, true);
            ctx.get().getParticipants().forEach(participant -> participant.tell(decision, getSelf()));
        }

        ctx.get().startTimer(this);
    }

    private void onVoteResponse(VoteResponse resp) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(resp);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        if (CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT == ctx.get().getProtocolState()) {
            // the vote response arrived too late
            return;
        }

        switch (resp.vote) {
            case YES: {
                ctx.get().addYesVoter(getSender());

                if (ctx.get().allVotedYes()) {
                    ctx.get().cancelTimer();
                    logger.info("GLOBAL_COMMIT");
                    ctx.get().setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.COMMIT);

                    // multicast GLOBAL_COMMIT to all participants
                    FinalDecision decision = new FinalDecision(resp.uuid(), FinalDecision.Decision.GLOBAL_COMMIT);
                    ctx.get().getParticipants().forEach(server -> getContext().system().scheduler().scheduleOnce(
                            Duration.ofSeconds(1), server, decision, getContext().dispatcher(), getSelf()));

                    // tell the client the result of the transaction
                    TxnResultMsg result = new TxnResultMsg(resp.uuid(), true);
                    ctx.get().getClient().tell(result, getSelf());
                }
                break;
            }
            case NO: {
                ctx.get().cancelTimer();
                logger.info("GLOBAL_ABORT");
                ctx.get().setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT);


                // multicast GLOBAL_ABORT to all participants
                FinalDecision decision = new FinalDecision(resp.uuid(), FinalDecision.Decision.GLOBAL_ABORT);
                ctx.get().getParticipants().forEach(server -> getContext().system().scheduler().scheduleOnce(
                        Duration.ofSeconds(1), server, decision, getContext().dispatcher(), getSelf()));

                // tell the client the result of the transaction
                TxnResultMsg result = new TxnResultMsg(resp.uuid(), false);
                ctx.get().getClient().tell(result, getSelf());

                break;
            }
        }
    }

    private void onTimeoutExpired(TimeoutExpired timeout) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(timeout);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        logger.info("GLOBAL_ABORT");
        ctx.get().setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT);

        FinalDecision decision = new FinalDecision(ctx.get().uuid, FinalDecision.Decision.GLOBAL_ABORT);
        ctx.get().getParticipants().forEach(server -> server.tell(decision, getSelf()));
    }

    private void onReadMsg(ReadMsg msg) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(msg);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        ReadRequest req = new ReadRequest(msg.uuid(), msg.key);

        ActorRef server = dispatcher.getServer(msg.key);
        server.tell(req, getSelf());

        ctx.get().addParticipant(server);
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

        ActorRef server = dispatcher.getServer(msg.key);
        server.tell(req, getSelf());

        ctx.get().addParticipant(server);
    }

    @Override
    protected void resume() {
        super.resume();
        List<CoordinatorRequestContext> active = getActive();
        List<CoordinatorRequestContext> decided = getDecided();
        active.forEach(this::recoveryAbort);
        decided.forEach(this::recoverySendDecision);
    }

    /**
     * This method implements a recovery action of the Two-phase commit (2PC) protocol.
     * If the coordinator has not yet taken the {@link FinalDecision} for the transaction, it aborts it.
     */
    private void recoveryAbort(CoordinatorRequestContext ctx) {
        ctx.setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT);
    }

    /**
     * This method implements a recovery action of the Two-phase commit (2PC) protocol.
     * If the coordinator already taken the {@link FinalDecision} for the transaction, it sends the decision to all
     * the participants.
     */
    private void recoverySendDecision(CoordinatorRequestContext ctx) {
        switch (ctx.getProtocolState()) {
            case COMMIT: {
                FinalDecision decision = new FinalDecision(ctx.uuid, FinalDecision.Decision.GLOBAL_COMMIT);
                ctx.getParticipants().forEach(participant -> participant.tell(decision, getSelf()));
                break;
            }
            case ABORT: {
                FinalDecision decision = new FinalDecision(ctx.uuid, FinalDecision.Decision.GLOBAL_ABORT);
                ctx.getParticipants().forEach(participants -> participants.tell(decision, getSelf()));
                break;
            }
        }
    }
}
