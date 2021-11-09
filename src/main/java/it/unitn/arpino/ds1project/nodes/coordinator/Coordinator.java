package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.messages.ServerInfo;
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
                .match(VoteResponseTimeout.class, this::onVoteResponseTimeout)
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
            logger.info(ctx.get().getClient().path().name() + " requested to commit");

            ctx.get().log(CoordinatorRequestContext.LogState.START_2PC);

            logger.info("Asking the VoteRequests to the participants");
            VoteRequest req = new VoteRequest(msg.uuid);
            ctx.get().getParticipants().forEach(participant -> participant.tell(req, getSelf()));

            ctx.get().setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.WAIT);
        } else {
            logger.info(ctx.get().getClient().path().name() + " requested to abort");

            ctx.get().log(CoordinatorRequestContext.LogState.GLOBAL_ABORT);

            logger.info("Sending the FinalDecision to the participants");
            FinalDecision decision = new FinalDecision(ctx.get().uuid, FinalDecision.Decision.GLOBAL_ABORT, true);
            ctx.get().getParticipants().forEach(participant -> participant.tell(decision, getSelf()));

            ctx.get().setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT);
        }

        ctx.get().startVoteResponseTimeout(this);
    }

    private void onVoteResponse(VoteResponse resp) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(resp);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        if (CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT == ctx.get().getProtocolState()) {
            logger.info("Received a VoteResponse, ignored as it arrived too late");
            return;
        }

        switch (resp.vote) {
            case YES: {
                logger.info("Received a YES vote from " + getSender().path().name());

                ctx.get().addYesVoter(getSender());

                if (ctx.get().allVotedYes()) {
                    ctx.get().cancelVoteResponseTimeout();

                    ctx.get().log(CoordinatorRequestContext.LogState.GLOBAL_COMMIT);

                    if (crash()) {
                        return;
                    }

                    logger.info("All voted YES. Sending the FinalDecision to the participants");
                    FinalDecision decision = new FinalDecision(resp.uuid, FinalDecision.Decision.GLOBAL_COMMIT);
                    ctx.get().getParticipants().forEach(server -> getContext().system().scheduler().scheduleOnce(
                            Duration.ofSeconds(1), server, decision, getContext().dispatcher(), getSelf()));

                    ctx.get().setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.COMMIT);

                    logger.info("Sending the transaction result to " + ctx.get().getClient().path().name());
                    TxnResultMsg result = new TxnResultMsg(resp.uuid, true);
                    ctx.get().getClient().tell(result, getSelf());
                }
                break;
            }
            case NO: {
                logger.info("Received a NO vote from " + getSender().path().name());

                ctx.get().cancelVoteResponseTimeout();

                ctx.get().log(CoordinatorRequestContext.LogState.GLOBAL_ABORT);

                logger.info("Sending the FinalDecision to the participants");
                FinalDecision decision = new FinalDecision(resp.uuid, FinalDecision.Decision.GLOBAL_ABORT);
                ctx.get().getParticipants().forEach(server -> getContext().system().scheduler().scheduleOnce(
                        Duration.ofSeconds(1), server, decision, getContext().dispatcher(), getSelf()));

                ctx.get().setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT);

                logger.info("Sending the transaction result to " + ctx.get().getClient().path().name());
                TxnResultMsg result = new TxnResultMsg(resp.uuid, false);
                ctx.get().getClient().tell(result, getSelf());

                break;
            }
        }
    }

    private void onVoteResponseTimeout(VoteResponseTimeout timeout) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(timeout);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        logger.info("Timeout expired. Reason: did not collect all VoteRequests from the participants in time");

        ctx.get().log(CoordinatorRequestContext.LogState.GLOBAL_ABORT);

        logger.info("Sending the transaction result to the participants");
        FinalDecision decision = new FinalDecision(ctx.get().uuid, FinalDecision.Decision.GLOBAL_ABORT);
        ctx.get().getParticipants().forEach(server -> server.tell(decision, getSelf()));

        ctx.get().setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT);

        logger.info("Sending the transaction result to " + ctx.get().getClient().path().name());
        ctx.get().getClient().tell(new TxnResultMsg(ctx.get().uuid, false), getSelf());
    }

    private void onReadMsg(ReadMsg msg) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(msg);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        ActorRef server = dispatcher.getServer(msg.key);

        if (!ctx.get().isParticipantPresent(server)) {
            ctx.get().addParticipant(server);

            // log before the tell, to minimize the actors' output interference
            logger.info("Added " + server.path().name() + " to the transaction's participants");
        }

        ReadRequest req = new ReadRequest(msg.uuid, msg.key);
        server.tell(req, getSelf());
    }

    private void onReadResult(ReadResult msg) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(msg);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        ReadResultMsg result = new ReadResultMsg(msg.uuid, msg.key, msg.value);

        ctx.get().getClient().tell(result, getSelf());
    }

    private void onWriteMsg(WriteMsg msg) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(msg);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        ActorRef server = dispatcher.getServer(msg.key);

        if (!ctx.get().isParticipantPresent(server)) {
            ctx.get().addParticipant(server);

            // log before the tell, to minimize the actors' output interference
            logger.info("Added " + server.path().name() + " to the transaction's participants");
        }

        WriteRequest req = new WriteRequest(msg.uuid, msg.key, msg.value);
        server.tell(req, getSelf());
    }

    @Override
    public void resume() {
        super.resume();
        List<CoordinatorRequestContext> active = getActive();
        List<CoordinatorRequestContext> decided = getDecided();

        // If the Coordinator has not yet taken the FinalDecision for the transaction, it aborts it.
        active.forEach(ctx -> {
            ctx.setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT);

            logger.info("Sending the transaction result to " + ctx.getClient().path().name());
            TxnResultMsg result = new TxnResultMsg(ctx.uuid, false);
            ctx.getClient().tell(result, getSelf());
        });

        // If the coordinator has already taken the FinalDecision for the transaction, it sends the decision to all
        // the participants.
        decided.forEach(ctx -> {
            switch ((CoordinatorRequestContext.LogState) ctx.loggedState().get()) {
                case GLOBAL_COMMIT: {
                    FinalDecision decision = new FinalDecision(ctx.uuid, FinalDecision.Decision.GLOBAL_COMMIT);
                    ctx.getParticipants().forEach(participant -> participant.tell(decision, getSelf()));
                    break;
                }
                case GLOBAL_ABORT: {
                    FinalDecision decision = new FinalDecision(ctx.uuid, FinalDecision.Decision.GLOBAL_ABORT);
                    ctx.getParticipants().forEach(participants -> participants.tell(decision, getSelf()));
                    break;
                }
            }
        });
    }
}
