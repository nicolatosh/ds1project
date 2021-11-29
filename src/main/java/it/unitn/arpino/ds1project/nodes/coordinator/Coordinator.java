package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.arpino.ds1project.messages.JoinMessage;
import it.unitn.arpino.ds1project.messages.ResumeMessage;
import it.unitn.arpino.ds1project.messages.client.ReadResultMsg;
import it.unitn.arpino.ds1project.messages.client.TxnAcceptMsg;
import it.unitn.arpino.ds1project.messages.client.TxnResultMsg;
import it.unitn.arpino.ds1project.messages.coordinator.*;
import it.unitn.arpino.ds1project.messages.server.*;
import it.unitn.arpino.ds1project.nodes.DataStoreNode;
import it.unitn.arpino.ds1project.simulation.Communication;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
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
    protected Receive createAliveReceive() {
        return receiveBuilder()
                .match(TxnBeginMsg.class, this::onTxnBeginMsg)
                .match(TxnEndMsg.class, this::onTxnEndMsg)
                .match(ReadMsg.class, this::onReadMsg)
                .match(ReadResult.class, this::onReadResult)
                .match(WriteMsg.class, this::onWriteMsg)
                .match(VoteResponse.class, this::onVoteResponse)
                .match(VoteResponseTimeout.class, this::onVoteResponseTimeout)
                .match(Done.class, this::onDone)
                .match(DoneTimeout.class, this::onDoneTimeout)
                .build();
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

    @Override
    protected void onJoinMessage(JoinMessage msg) {
        logger.info(getSender().path().name() + " joined");
        IntStream.rangeClosed(msg.lowerKey, msg.upperKey).forEach(key -> dispatcher.map(key, getSender()));
    }

    private void onTxnBeginMsg(TxnBeginMsg msg) {
        CoordinatorRequestContext ctx = this.createNewContext();
        ctx.log(CoordinatorRequestContext.LogState.CONVERSATIONAL);

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

        switch (ctx.get().loggedState()) {
            case CONVERSATIONAL: {
                if (msg.commit) {
                    logger.info(ctx.get().getClient().path().name() + " requested to commit");

                    ctx.get().log(CoordinatorRequestContext.LogState.START_2PC);

                    logger.info("Asking the vote requests to the participants");
                    Communication multicast = Communication.builder()
                            .ofSender(getSelf())
                            .ofReceivers(ctx.get().getParticipants())
                            .ofMessage(new VoteRequest(msg.uuid))
                            .ofCrashProbability(getParameters().coordinatorOnVoteRequestCrashProbability);
                    if (!multicast.run()) {
                        logger.info("Did not send the message to " + multicast.getMissing().stream()
                                .map(participant -> participant.path().name())
                                .collect(Collectors.joining(", ")));
                        crash();
                        return;
                    }

                    ctx.get().startVoteResponseTimer(this);

                    ctx.get().setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.WAIT);
                } else {
                    logger.info(ctx.get().getClient().path().name() + " requested to abort");

                    ctx.get().log(CoordinatorRequestContext.LogState.GLOBAL_ABORT);

                    logger.info("Sending the final decision to the participants");
                    Communication multicast = Communication.builder()
                            .ofSender(getSelf())
                            .ofReceivers(ctx.get().getParticipants())
                            .ofMessage(new FinalDecision(ctx.get().uuid, FinalDecision.Decision.GLOBAL_ABORT))
                            .ofCrashProbability(getParameters().coordinatorOnFinalDecisionCrashProbability);
                    if (!multicast.run()) {
                        logger.info("Did not send the message to " + multicast.getMissing().stream()
                                .map(participant -> participant.path().name())
                                .collect(Collectors.joining(", ")));
                        crash();
                        return;
                    }

                    ctx.get().setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT);

                    logger.info("Sending the transaction result to " + ctx.get().getClient().path().name());
                    ctx.get().getClient().tell(new TxnResultMsg(ctx.get().uuid, false), getSelf());

                    // if we received a client abort, we do not have to start the Two-phase commit (2PC) protocol,
                    // thus we must not start the vote response timer.
                    // Instead, we should wait for the Done messages to arrive in order to remove the transaction.
                    ctx.get().startDoneRequestTimer(this);
                }
                break;
            }
            case START_2PC: {
                // the coordinator is still running the Two-phase commit (2PC) protocol:
                // it cannot reply with a meaningful answer
                break;
            }
            case GLOBAL_COMMIT: {
                FinalDecision decision = new FinalDecision(ctx.get().uuid, FinalDecision.Decision.GLOBAL_COMMIT);
                getSender().tell(decision, getSelf());
                break;
            }
            case GLOBAL_ABORT: {
                FinalDecision decision = new FinalDecision(ctx.get().uuid, FinalDecision.Decision.GLOBAL_ABORT);
                getSender().tell(decision, getSelf());
                break;
            }
        }
    }

    private void onVoteResponse(VoteResponse resp) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(resp.uuid);
        if (ctx.isEmpty()) {
            logger.severe("Bad request");
            return;
        }

        switch (ctx.get().loggedState()) {
            case CONVERSATIONAL: {
                logger.severe("Invalid logged state (CONVERSATIONAL)");
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
                            Communication multicast = Communication.builder()
                                    .ofSender(getSelf())
                                    .ofReceivers(ctx.get().getParticipants())
                                    .ofMessage(new FinalDecision(resp.uuid, FinalDecision.Decision.GLOBAL_COMMIT))
                                    .ofCrashProbability(getParameters().coordinatorOnFinalDecisionCrashProbability);
                            if (!multicast.run()) {
                                logger.info("Did not send the message to " + multicast.getMissing().stream()
                                        .map(participant -> participant.path().name())
                                        .collect(Collectors.joining(", ")));
                                crash();
                                return;
                            }

                            ctx.get().setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.COMMIT);

                            TxnResultMsg result = new TxnResultMsg(ctx.get().uuid, true);
                            ctx.get().getClient().tell(result, getSelf());
                        }
                        break;
                    }
                    case NO: {
                        logger.info("Received a NO vote from " + getSender().path().name());

                        ctx.get().cancelVoteResponseTimeout();

                        ctx.get().log(CoordinatorRequestContext.LogState.GLOBAL_ABORT);

                        logger.info("Sending the final decision to the participants");
                        Communication multicast = Communication.builder()
                                .ofSender(getSelf())
                                .ofReceivers(ctx.get().getParticipants())
                                .ofMessage(new FinalDecision(resp.uuid, FinalDecision.Decision.GLOBAL_ABORT))
                                .ofCrashProbability(getParameters().coordinatorOnVoteResponseCrashProbability);
                        if (!multicast.run()) {
                            logger.info("Did not send the message to " + multicast.getMissing().stream()
                                    .map(participant -> participant.path().name())
                                    .collect(Collectors.joining(", ")));
                            crash();
                            return;
                        }

                        ctx.get().setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT);

                        logger.info("Sending the transaction result to the client");
                        TxnResultMsg result = new TxnResultMsg(ctx.get().uuid, false);
                        ctx.get().getClient().tell(result, getSelf());

                        break;
                    }
                }
                // wait for the Done messages to arrive in order to remove the transaction.
                ctx.get().startDoneRequestTimer(this);

                break;
            }
            case GLOBAL_COMMIT: {
                logger.severe("Invalid logged state (GLOBAL_COMMIT)");
                return;
            }
            case GLOBAL_ABORT: {
                logger.info("Received a VoteResponse, ignored as it arrived too late");
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
        Communication multicast = Communication.builder()
                .ofSender(getSelf())
                .ofReceivers(ctx.get().getParticipants())
                .ofMessage(new FinalDecision(ctx.get().uuid, FinalDecision.Decision.GLOBAL_ABORT))
                .ofCrashProbability(getParameters().coordinatorOnFinalDecisionCrashProbability);
        if (!multicast.run()) {
            logger.info("Did not send the message to " + multicast.getMissing().stream()
                    .map(participant -> participant.path().name())
                    .collect(Collectors.joining(", ")));
            crash();
            return;
        }

        ctx.get().setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT);

        // wait for the Done messages to arrive in order to remove the transaction.
        ctx.get().startDoneRequestTimer(this);

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

        if (ctx.get().loggedState() != CoordinatorRequestContext.LogState.CONVERSATIONAL) {
            logger.severe("Invalid logged state (" + ctx.get().loggedState() + ", should be CONVERSATIONAL)");
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

        if (ctx.get().loggedState() != CoordinatorRequestContext.LogState.CONVERSATIONAL) {
            logger.severe("Invalid logged state (" + ctx.get().loggedState() + ", should be CONVERSATIONAL)");
            return;
        }

        ActorRef server = dispatcher.getServer(msg.key);

        ctx.get().addParticipant(server);

        WriteRequest req = new WriteRequest(msg.uuid, msg.key, msg.value);
        server.tell(req, getSelf());
    }

    private void onDone(Done msg) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(msg.uuid);
        if (ctx.isEmpty()) {
            logger.severe("Bad request");
            return;
        }

        if (!ctx.get().isDecided()) {
            logger.severe("Invalid logged state (" + ctx.get().loggedState() + ", should be GLOBAL_COMMIT or GLOBAL_ABORT)");
            return;
        }

        ctx.get().addDoneParticipant(getSender());

        if (ctx.get().allParticipantsDone()) {
            ctx.get().cancelDoneRequestTimer();

            logger.info("All Done messages arrived. Removing the context");
            removeContext(ctx.get());
        } else {
            Collection<ActorRef> missing = ctx.get().getMissingDoneParticipants();
            logger.info(missing.size() + " Done messages required left, from " + missing.stream()
                    .map(participant -> participant.path().name())
                    .collect(Collectors.joining(", ")));
        }
    }

    private void onDoneTimeout(DoneTimeout timeout) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(timeout.uuid);
        if (ctx.isEmpty()) {
            return;
        }

        if (!ctx.get().isDecided()) {
            logger.severe("Invalid logged state (" + ctx.get().loggedState() + ", should be GLOBAL_COMMIT or GLOBAL_ABORT)");
            return;
        }

        logger.info("Soliciting the participants");

        Solicit solicit = new Solicit(ctx.get().uuid);
        ctx.get().getMissingDoneParticipants().forEach(participant -> participant.tell(solicit, getSelf()));

        ctx.get().startDoneRequestTimer(this);
    }

    @Override
    protected void crash() {
        super.crash();

        if (getParameters().coordinatorRecoveryTimeS >= 0) {
            getContext().system().scheduler().scheduleOnce(
                    Duration.ofSeconds(getParameters().coordinatorRecoveryTimeS), // delay
                    getSelf(), // receiver
                    new ResumeMessage(), // message
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

            ctx.startDoneRequestTimer(this);

            TxnResultMsg result = new TxnResultMsg(ctx.uuid, false);
            ctx.getClient().tell(result, getSelf());
        });

        // If we have already taken the final decision for the transaction, we send it to all the participants.
        decided.forEach(ctx -> {
            switch (ctx.loggedState()) {
                case GLOBAL_COMMIT: {
                    logger.info("Sending the final decision to the participants");
                    Communication multicast = Communication.builder()
                            .ofSender(getSelf())
                            .ofReceivers(ctx.getParticipants())
                            .ofMessage(new FinalDecision(ctx.uuid, FinalDecision.Decision.GLOBAL_COMMIT))
                            .ofCrashProbability(getParameters().coordinatorOnFinalDecisionCrashProbability);
                    if (!multicast.run()) {
                        logger.info("Did not send the message to " + multicast.getMissing().stream()
                                .map(participant -> participant.path().name())
                                .collect(Collectors.joining(", ")));
                        crash();
                        return;
                    }
                    ctx.setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.COMMIT);

                    TxnResultMsg result = new TxnResultMsg(ctx.uuid, true);
                    ctx.getClient().tell(result, getSelf());

                    ctx.startDoneRequestTimer(this);

                    break;
                }
                case GLOBAL_ABORT: {
                    logger.info("Sending the final decision to the participants");
                    Communication multicast = Communication.builder()
                            .ofSender(getSelf())
                            .ofReceivers(ctx.getParticipants())
                            .ofMessage(new FinalDecision(ctx.uuid, FinalDecision.Decision.GLOBAL_ABORT))
                            .ofCrashProbability(getParameters().coordinatorOnFinalDecisionCrashProbability);
                    if (!multicast.run()) {
                        logger.info("Did not send the message to " + multicast.getMissing().stream()
                                .map(participant -> participant.path().name())
                                .collect(Collectors.joining(", ")));
                        crash();
                        return;
                    }
                    ctx.setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT);

                    ctx.startDoneRequestTimer(this);

                    TxnResultMsg result = new TxnResultMsg(ctx.uuid, true);
                    ctx.getClient().tell(result, getSelf());

                    break;
                }
            }
        });
    }
}
