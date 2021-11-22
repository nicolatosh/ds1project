package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.messages.Resume;
import it.unitn.arpino.ds1project.messages.client.ReadResultMsg;
import it.unitn.arpino.ds1project.messages.client.TxnAcceptMsg;
import it.unitn.arpino.ds1project.messages.client.TxnResultMsg;
import it.unitn.arpino.ds1project.messages.coordinator.*;
import it.unitn.arpino.ds1project.messages.server.FinalDecision;
import it.unitn.arpino.ds1project.messages.server.ReadRequest;
import it.unitn.arpino.ds1project.messages.server.VoteRequest;
import it.unitn.arpino.ds1project.messages.server.WriteRequest;
import it.unitn.arpino.ds1project.nodes.DataStoreNode;
import it.unitn.arpino.ds1project.simulation.Communication;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

public class Coordinator extends DataStoreNode<CoordinatorRequestContext> {
    private final Dispatcher dispatcher;

    public Coordinator() {
        dispatcher = new Dispatcher();
    }

    public static Props props() {
        return Props.create(Coordinator.class, Coordinator::new);
    }

    @Override
    public Receive createReceive() {
        return new ReceiveBuilder()
                .match(ServerJoin.class, this::onServerJoined)
                .match(TxnBeginMsg.class, this::onTxnBeginMsg)
                .match(TxnEndMsg.class, this::onTxnEndMsg)
                .match(ReadMsg.class, this::onReadMsg)
                .match(ReadResult.class, this::onReadResult)
                .match(WriteMsg.class, this::onWriteMsg)
                .match(VoteResponse.class, this::onVoteResponse)
                .match(VoteResponseTimeout.class, this::onVoteResponseTimeout)
                .build();
    }

    public Dispatcher getDispatcher() {
        return dispatcher;
    }

    /**
     * Creates a new context and adds it to the list of contexts so that it can be later retrieved.
     *
     * @return The newly created context.
     */
    private CoordinatorRequestContext createNewContext() {
        CoordinatorRequestContext ctx = new CoordinatorRequestContext(getSender());

        addContext(ctx);
        return ctx;
    }

    private void onServerJoined(ServerJoin msg) {
        IntStream.rangeClosed(msg.lowerKey, msg.upperKey).forEach(key -> dispatcher.map(key, getSender()));
    }

    private void onTxnBeginMsg(TxnBeginMsg msg) {
        CoordinatorRequestContext ctx = this.createNewContext();

        ctx.log(CoordinatorRequestContext.LogState.NONE);

        TxnAcceptMsg response = new TxnAcceptMsg(ctx.uuid);
        getSender().tell(response, getSelf());

        ctx.setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.INIT);
    }

    private void onTxnEndMsg(TxnEndMsg msg) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(msg.uuid);
        if (ctx.isEmpty()) {
            logger.severe("Bad request");
            return;
        }

        if (msg.commit) {
            logger.info(ctx.get().getClient().path().name() + " requested to commit");

            ctx.get().log(CoordinatorRequestContext.LogState.START_2PC);

            logger.info("Asking the vote requests to the participants");
            VoteRequest req = new VoteRequest(msg.uuid);
            if (!Communication.multicast(getSelf(), ctx.get().getParticipants(), req, getParameters().coordinatorOnVoteRequestCrashProbability)) {
                crash();
                return;
            }

            ctx.get().startVoteResponseTimer(this);

            ctx.get().setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.WAIT);
        } else {
            logger.info(ctx.get().getClient().path().name() + " requested to abort");

            ctx.get().log(CoordinatorRequestContext.LogState.GLOBAL_ABORT);

            logger.info("Sending the final decision to the participants");
            FinalDecision decision = new FinalDecision(ctx.get().uuid, FinalDecision.Decision.GLOBAL_ABORT);
            if (!Communication.multicast(getSelf(), ctx.get().getParticipants(), decision, 0.)) {
                crash();
                return;
            }

            ctx.get().setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT);

            logger.info("Sending the transaction result to " + ctx.get().getClient().path().name());
            ctx.get().getClient().tell(new TxnResultMsg(ctx.get().uuid, false), getSelf());

            // if we received a client abort, we do not have to start the Two-phase commit (2PC) protocol,
            // thus we must not start the vote response timer.
        }
    }

    private void onVoteResponse(VoteResponse resp) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(resp.uuid);
        if (ctx.isEmpty()) {
            logger.severe("Bad request");
            return;
        }

        switch (ctx.get().loggedState()) {
            case NONE: {
                logger.severe("Invalid logged state (NONE)");
                return;
            }
            case START_2PC: {
                switch (resp.vote) {
                    case YES: {
                        logger.info("Received a YES vote from " + getSender().path().name());

                        ctx.get().addYesVoter(getSender());

                        if (ctx.get().allVotedYes()) {
                            ctx.get().cancelVoteResponseTimeout();

                            ctx.get().log(CoordinatorRequestContext.LogState.GLOBAL_COMMIT);

                            logger.info("All voted YES. Sending the final decision to the participants");
                            FinalDecision decision = new FinalDecision(resp.uuid, FinalDecision.Decision.GLOBAL_COMMIT);
                            if (!Communication.multicast(getSelf(), ctx.get().getParticipants(), decision, getParameters().coordinatorOnFinalDecisionCrashProbability)) {
                                crash();
                                return;
                            }

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

                        logger.info("Sending the final decision to the participants");
                        FinalDecision decision = new FinalDecision(resp.uuid, FinalDecision.Decision.GLOBAL_ABORT);
                        if (!Communication.multicast(getSelf(), ctx.get().getParticipants(), decision, getParameters().coordinatorOnVoteResponseCrashProbability)) {
                            crash();
                            return;
                        }

                        ctx.get().setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT);

                        logger.info("Sending the transaction result to " + ctx.get().getClient().path().name());
                        TxnResultMsg result = new TxnResultMsg(resp.uuid, false);
                        ctx.get().getClient().tell(result, getSelf());

                        break;
                    }
                }
                break;
            }
            case GLOBAL_COMMIT: {
                logger.severe("Invalid logged state (GLOBAL_COMMIT)");
                return;
            }
            case GLOBAL_ABORT: {
                logger.info("Received a VoteResponse, ignored as it arrived too late");
                return;
            }
        }
    }

    private void onVoteResponseTimeout(VoteResponseTimeout timeout) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(timeout.uuid);
        if (ctx.isEmpty()) {
            logger.severe("Bad request");
            return;
        }

        if (ctx.get().loggedState() != CoordinatorRequestContext.LogState.START_2PC) {
            logger.severe("Invalid logged state (" + ctx.get().loggedState() + ", should be START_2PC)");
            return;
        }

        logger.info("Aborting the transaction");

        ctx.get().log(CoordinatorRequestContext.LogState.GLOBAL_ABORT);

        logger.info("Sending the final decision to the participants");
        FinalDecision decision = new FinalDecision(ctx.get().uuid, FinalDecision.Decision.GLOBAL_ABORT);
        if (!Communication.multicast(getSelf(), ctx.get().getParticipants(), decision, 0.)) {
            crash();
            return;
        }

        ctx.get().setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT);

        logger.info("Sending the transaction result to " + ctx.get().getClient().path().name());
        TxnResultMsg result = new TxnResultMsg(ctx.get().uuid, false);
        ctx.get().getClient().tell(result, getSelf());
    }

    private void onReadMsg(ReadMsg msg) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(msg.uuid);
        if (ctx.isEmpty()) {
            logger.severe("Bad request");
            return;
        }

        ActorRef server = dispatcher.getServer(msg.key);

        ctx.get().addParticipant(server);

        ReadRequest req = new ReadRequest(msg.uuid, msg.key);
        server.tell(req, getSelf());
    }

    private void onReadResult(ReadResult msg) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(msg.uuid);
        if (ctx.isEmpty()) {
            logger.severe("Bad request");
            return;
        }

        if (ctx.get().loggedState() != CoordinatorRequestContext.LogState.NONE) {
            logger.severe("Invalid logged state (" + ctx.get().loggedState() + ", should be NONE)");
            return;
        }

        ReadResultMsg result = new ReadResultMsg(msg.uuid, msg.key, msg.value);

        ctx.get().getClient().tell(result, getSelf());
    }

    private void onWriteMsg(WriteMsg msg) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(msg.uuid);
        if (ctx.isEmpty()) {
            logger.severe("Bad request");
            return;
        }

        if (ctx.get().loggedState() != CoordinatorRequestContext.LogState.NONE) {
            logger.severe("Invalid logged state (" + ctx.get().loggedState() + ", should be NONE)");
            return;
        }

        ActorRef server = dispatcher.getServer(msg.key);

        ctx.get().addParticipant(server);

        WriteRequest req = new WriteRequest(msg.uuid, msg.key, msg.value);
        server.tell(req, getSelf());
    }

    @Override
    protected void crash() {
        super.crash();

        if (getParameters().coordinatorRecoveryTimeS >= 0) {
            getContext().system().scheduler().scheduleOnce(
                    Duration.ofSeconds(getParameters().coordinatorRecoveryTimeS), // delay
                    getSelf(), // receiver
                    new Resume(), // message
                    getContext().dispatcher(), // executor
                    ActorRef.noSender()); // sender
        }
    }

    @Override
    public void resume() {
        super.resume();

        List<CoordinatorRequestContext> active = getActive();
        List<CoordinatorRequestContext> decided = getDecided();

        // If we have not yet taken the final decision for the transaction, we abort it,
        // and send the transaction result to the client.
        active.forEach(ctx -> {
            logger.info("Aborting the transaction");
            ctx.log(CoordinatorRequestContext.LogState.GLOBAL_ABORT);
            ctx.setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT);

            logger.info("Sending the transaction result to " + ctx.getClient().path().name());
            TxnResultMsg result = new TxnResultMsg(ctx.uuid, false);
            ctx.getClient().tell(result, getSelf());
        });

        // If we have already taken the final decision for the transaction, we send it to all the participants
        // and send the transaction result to the client.
        decided.forEach(ctx -> {
            switch (ctx.loggedState()) {
                case GLOBAL_COMMIT: {
                    logger.info("Sending the final decision to the participants");
                    FinalDecision decision = new FinalDecision(ctx.uuid, FinalDecision.Decision.GLOBAL_COMMIT);
                    if (!Communication.multicast(getSelf(), ctx.getParticipants(), decision, 0.)) {
                        crash();
                        return;
                    }
                    ctx.setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.COMMIT);

                    logger.info("Sending the transaction result to " + ctx.getClient().path().name());
                    TxnResultMsg result = new TxnResultMsg(ctx.uuid, true);
                    ctx.getClient().tell(result, getSelf());

                    break;
                }
                case GLOBAL_ABORT: {
                    logger.info("Sending the final decision to the participants");
                    FinalDecision decision = new FinalDecision(ctx.uuid, FinalDecision.Decision.GLOBAL_ABORT);
                    if (!Communication.multicast(getSelf(), ctx.getParticipants(), decision, 0.)) {
                        crash();
                        return;
                    }
                    ctx.setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT);

                    logger.info("Sending the transaction result to " + ctx.getClient().path().name());
                    TxnResultMsg result = new TxnResultMsg(ctx.uuid, false);
                    ctx.getClient().tell(result, getSelf());

                    break;
                }
            }
        });
    }
}
