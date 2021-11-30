package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.arpino.ds1project.messages.JoinMessage;
import it.unitn.arpino.ds1project.messages.ResumeMessage;
import it.unitn.arpino.ds1project.messages.TxnMessage;
import it.unitn.arpino.ds1project.messages.client.ReadResultMsg;
import it.unitn.arpino.ds1project.messages.client.Reset;
import it.unitn.arpino.ds1project.messages.client.TxnAcceptMsg;
import it.unitn.arpino.ds1project.messages.client.TxnResultMsg;
import it.unitn.arpino.ds1project.messages.coordinator.*;
import it.unitn.arpino.ds1project.messages.server.FinalDecision;
import it.unitn.arpino.ds1project.messages.server.ReadRequest;
import it.unitn.arpino.ds1project.messages.server.VoteRequest;
import it.unitn.arpino.ds1project.messages.server.WriteRequest;
import it.unitn.arpino.ds1project.nodes.DataStoreNode;
import it.unitn.arpino.ds1project.simulation.Communication;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.time.Duration;
import java.util.Collection;
import java.util.UUID;
import java.util.function.Predicate;
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
    public void aroundReceive(PartialFunction<Object, BoxedUnit> receive, Object obj) {
        super.aroundReceive(receive, obj);

        if (obj instanceof TxnMessage) {
            var msg = (TxnMessage) obj;

            if (!(msg instanceof TxnBeginMsg)) {

                // there must exist a context associated with this message
                if (!getRepository().existsContextWithId(msg.uuid)) {
                    logger.severe("Bad request");

                    var reset = new Reset(msg.uuid);
                    getSender().tell(reset, getSelf());
                }
            }
        }
    }

    @Override
    protected Receive createAliveReceive() {
        return receiveBuilder()
                .match(TxnBeginMsg.class, this::onTxnBeginMsg)
                .match(TxnEndMsg.class, this::onTxnEndMsg)
                .match(TxnEndTimeout.class, this::onTxnEndTimeout)
                .match(ReadMsg.class, this::onReadMsg)
                .match(ReadResult.class, this::onReadResult)
                .match(WriteMsg.class, this::onWriteMsg)
                .match(VoteResponse.class, this::onVoteResponse)
                .match(VoteResponseTimeout.class, this::onVoteResponseTimeout)
                .build();
    }

    /**
     * Creates a new context and adds it to the list of contexts so that it can be later retrieved.
     *
     * @return The newly created context.
     */
    private CoordinatorRequestContext createNewContext() {
        CoordinatorRequestContext ctx = new CoordinatorRequestContext(UUID.randomUUID(), getSender());

        getRepository().addRequestContext(ctx);
        return ctx;
    }

    @Override
    protected void onJoinMessage(JoinMessage msg) {
        logger.info(getSender().path().name() + " joined");
        IntStream.rangeClosed(msg.lowerKey, msg.upperKey).forEach(key -> dispatcher.map(key, getSender()));
    }

    private void onTxnBeginMsg(TxnBeginMsg msg) {
        if (getRepository().existsContextWithId(msg.uuid)) {
            var ctx = getRepository().getRequestContextById(msg.uuid);
            if (ctx.isDecided()) {
                logger.severe("Context is already decided");
                return;
            }
            var accept = new TxnAcceptMsg(ctx.uuid);
            getSender().tell(accept, getSelf());
        }

        var ctx = new CoordinatorRequestContext(msg.uuid, getSender());
        getRepository().addRequestContext(ctx);

        ctx.log(CoordinatorRequestContext.LogState.CONVERSATIONAL);

        var accept = new TxnAcceptMsg(ctx.uuid);
        getSender().tell(accept, getSelf());

        ctx.setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.INIT);

        ctx.startTxnEndTimer(this);
    }

    private void onTxnEndMsg(TxnEndMsg msg) {
        var ctx = getRepository().getRequestContextById(msg.uuid);

        switch (ctx.loggedState()) {
            case CONVERSATIONAL: {
                ctx.cancelTxnEndTimer();
                if (msg.commit) {
                    logger.info(ctx.subject.path().name() + " requested to commit");

                    ctx.log(CoordinatorRequestContext.LogState.START_2PC);

                    logger.info("Asking the vote requests to the participants");
                    Communication multicast = Communication.builder()
                            .ofSender(getSelf())
                            .ofReceivers(ctx.getParticipants())
                            .ofMessage(new VoteRequest(msg.uuid))
                            .ofCrashProbability(getParameters().coordinatorOnVoteRequestCrashProbability);
                    if (!multicast.run()) {
                        logger.info("Did not send the message to " + multicast.getMissing().stream()
                                .map(participant -> participant.path().name())
                                .collect(Collectors.joining(", ")));
                        crash();
                        return;
                    }

                    ctx.startVoteResponseTimer(this);

                    ctx.setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.WAIT);
                } else {
                    logger.info(ctx.subject.path().name() + " requested to abort");

                    ctx.log(CoordinatorRequestContext.LogState.GLOBAL_ABORT);

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

                    logger.info("Sending the transaction result to " + ctx.subject.path().name());
                    TxnResultMsg result = new TxnResultMsg(ctx.uuid, false);
                    ctx.subject.tell(result, getSelf());

                    // if we received a client abort, we do not have to start the Two-phase commit (2PC) protocol,
                    // thus we must not start the vote response timer.
                }
                break;
            }
            case START_2PC: {
                // the coordinator is still running the Two-phase commit (2PC) protocol:
                // it cannot reply with a meaningful answer
                break;
            }
            case GLOBAL_COMMIT: {
                FinalDecision decision = new FinalDecision(ctx.uuid, FinalDecision.Decision.GLOBAL_COMMIT);
                getSender().tell(decision, getSelf());
                break;
            }
            case GLOBAL_ABORT: {
                FinalDecision decision = new FinalDecision(ctx.uuid, FinalDecision.Decision.GLOBAL_ABORT);
                getSender().tell(decision, getSelf());
                break;
            }
        }
    }

    private void onTxnEndTimeout(TxnEndTimeout timeout) {
        var ctx = getRepository().getRequestContextById(timeout.uuid);

        if (ctx.loggedState() != CoordinatorRequestContext.LogState.CONVERSATIONAL) {
            logger.severe("Invalid logged state (" + ctx.loggedState() + ", should be CONVERSATIONAL)");
            return;
        }

        var decision = new FinalDecision(ctx.uuid, FinalDecision.Decision.GLOBAL_ABORT);
        var multicast = Communication.builder()
                .ofSender(getSelf())
                .ofReceivers(ctx.getParticipants())
                .ofMessage(decision)
                .ofCrashProbability(getParameters().coordinatorOnFinalDecisionCrashProbability);
        if (!multicast.run()) {
            logger.info("Did not send the message to " + multicast.getMissing().stream()
                    .map(participant -> participant.path().name())
                    .collect(Collectors.joining(", ")));
            crash();
            return;
        }

        ctx.setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT);

        // Do not send the transaction result to the client!
        // The client will send a TxnEndMsg later, to which we will immediately respond with a global abort
    }

    private void onVoteResponse(VoteResponse resp) {
        var ctx = getRepository().getRequestContextById(resp.uuid);

        switch (ctx.loggedState()) {
            case CONVERSATIONAL: {
                logger.severe("Invalid logged state (CONVERSATIONAL)");
                return;
            }
            case START_2PC: {
                switch (resp.vote) {
                    case YES: {
                        logger.info("Received a YES vote from " + getSender().path().name());

                        ctx.addYesVoter(getSender());

                        if (ctx.allVotedYes()) {
                            ctx.cancelVoteResponseTimer();

                            ctx.log(CoordinatorRequestContext.LogState.GLOBAL_COMMIT);

                            logger.info("All voted YES. Sending the final decision to the participants");
                            Communication multicast = Communication.builder()
                                    .ofSender(getSelf())
                                    .ofReceivers(ctx.getParticipants())
                                    .ofMessage(new FinalDecision(resp.uuid, FinalDecision.Decision.GLOBAL_COMMIT))
                                    .ofCrashProbability(getParameters().coordinatorOnFinalDecisionCrashProbability);
                            if (!multicast.run()) {
                                logger.info("Did not send the message to " + multicast.getMissing().stream()
                                        .map(participant -> participant.path().name())
                                        .collect(Collectors.joining(", ")));
                                crash();
                                return;
                            }

                            ctx.setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.COMMIT);

                            logger.info("Sending the transaction result to " + ctx.subject.path().name());
                            TxnResultMsg result = new TxnResultMsg(ctx.uuid, true);
                            ctx.subject.tell(result, getSelf());
                        }
                        break;
                    }
                    case NO: {
                        logger.info("Received a NO vote from " + getSender().path().name());

                        ctx.cancelVoteResponseTimer();

                        ctx.log(CoordinatorRequestContext.LogState.GLOBAL_ABORT);

                        logger.info("Sending the final decision to the participants");
                        Communication multicast = Communication.builder()
                                .ofSender(getSelf())
                                .ofReceivers(ctx.getParticipants())
                                .ofMessage(new FinalDecision(resp.uuid, FinalDecision.Decision.GLOBAL_ABORT))
                                .ofCrashProbability(getParameters().coordinatorOnFinalDecisionCrashProbability);
                        if (!multicast.run()) {
                            logger.info("Did not send the message to " + multicast.getMissing().stream()
                                    .map(participant -> participant.path().name())
                                    .collect(Collectors.joining(", ")));
                            crash();
                            return;
                        }

                        ctx.setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT);

                        logger.info("Sending the transaction result to " + ctx.subject.path().name());
                        TxnResultMsg result = new TxnResultMsg(ctx.uuid, false);
                        ctx.subject.tell(result, getSelf());

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
            }
        }
    }

    private void onVoteResponseTimeout(VoteResponseTimeout timeout) {
        var ctx = getRepository().getRequestContextById(timeout.uuid);

        if (ctx.loggedState() != CoordinatorRequestContext.LogState.START_2PC) {
            logger.severe("Invalid logged state (" + ctx.loggedState() + ", should be START_2PC)");
            return;
        }

        logger.info("Aborting the transaction");

        ctx.log(CoordinatorRequestContext.LogState.GLOBAL_ABORT);

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

        logger.info("Sending the transaction result to " + ctx.subject.path().name());
        TxnResultMsg result = new TxnResultMsg(ctx.uuid, false);
        ctx.subject.tell(result, getSelf());
    }

    private void onReadMsg(ReadMsg msg) {
        var ctx = getRepository().getRequestContextById(msg.uuid);

        ActorRef server = dispatcher.getServer(msg.key);

        ctx.addParticipant(server);

        ReadRequest req = new ReadRequest(msg.uuid, msg.key);
        server.tell(req, getSelf());
    }

    private void onReadResult(ReadResult msg) {
        var ctx = getRepository().getRequestContextById(msg.uuid);

        if (ctx.loggedState() != CoordinatorRequestContext.LogState.CONVERSATIONAL) {
            logger.severe("Invalid logged state (" + ctx.loggedState() + ", should be CONVERSATIONAL)");
            return;
        }

        ReadResultMsg result = new ReadResultMsg(msg.uuid, msg.key, msg.value);

        ctx.subject.tell(result, getSelf());
    }

    private void onWriteMsg(WriteMsg msg) {
        var ctx = getRepository().getRequestContextById(msg.uuid);

        if (ctx.loggedState() != CoordinatorRequestContext.LogState.CONVERSATIONAL) {
            logger.severe("Invalid logged state (" + ctx.loggedState() + ", should be CONVERSATIONAL)");
            return;
        }

        ActorRef server = dispatcher.getServer(msg.key);

        ctx.addParticipant(server);

        WriteRequest req = new WriteRequest(msg.uuid, msg.key, msg.value);
        server.tell(req, getSelf());
    }

    @Override
    protected void crash() {
        super.crash();

        getRepository().getAllRequestContexts().forEach(CoordinatorRequestContext::cancelVoteResponseTimer);

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

        Collection<CoordinatorRequestContext> active = getRepository().getAllRequestContexts(Predicate.not(CoordinatorRequestContext::isDecided));
        Collection<CoordinatorRequestContext> decided = getRepository().getAllRequestContexts(CoordinatorRequestContext::isDecided);

        // If we have not yet taken the final decision for the transaction, we abort it,
        // and send the transaction result to the client.
        active.forEach(ctx -> {
            logger.info("Aborting the transaction");
            ctx.log(CoordinatorRequestContext.LogState.GLOBAL_ABORT);
            ctx.setProtocolState(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT);

            logger.info("Sending the transaction result to " + ctx.subject.path().name());
            TxnResultMsg result = new TxnResultMsg(ctx.uuid, false);
            ctx.subject.tell(result, getSelf());
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

                    logger.info("Sending the transaction result to " + ctx.subject.path().name());
                    TxnResultMsg result = new TxnResultMsg(ctx.uuid, true);
                    ctx.subject.tell(result, getSelf());

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

                    logger.info("Sending the transaction result to " + ctx.subject.path().name());
                    TxnResultMsg result = new TxnResultMsg(ctx.uuid, true);
                    ctx.subject.tell(result, getSelf());

                    break;
                }
            }
        });
    }
}
