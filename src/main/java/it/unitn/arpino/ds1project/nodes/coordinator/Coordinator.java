package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.messages.*;
import it.unitn.arpino.ds1project.messages.client.ReadResultMsg;
import it.unitn.arpino.ds1project.messages.client.TxnAcceptMsg;
import it.unitn.arpino.ds1project.messages.client.TxnResultMsg;
import it.unitn.arpino.ds1project.messages.coordinator.*;
import it.unitn.arpino.ds1project.messages.server.*;
import it.unitn.arpino.ds1project.nodes.DataStoreNode;
import it.unitn.arpino.ds1project.simulation.Communication;
import it.unitn.arpino.ds1project.simulation.CoordinatorParameters;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.time.Duration;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Coordinator extends DataStoreNode<CoordinatorRequestContext> {
    private final CoordinatorParameters parameters;

    private final Dispatcher dispatcher;

    public Coordinator() {
        parameters = new CoordinatorParameters();
        dispatcher = new Dispatcher();
    }

    public static Props props() {
        return Props.create(Coordinator.class, Coordinator::new).withDispatcher("my-pinned-dispatcher");
    }

    public CoordinatorParameters getParameters() {
        return parameters;
    }

    @Override
    public void aroundReceive(PartialFunction<Object, BoxedUnit> receive, Object obj) {
        if (obj instanceof TxnMessage) {
            var msg = (TxnMessage) obj;

            switch (getStatus()) {
                case ALIVE: {
                    logger.info("Received " + msg + " from " + getSender().path().name());
                    break;
                }
                case CRASHED: {
                    logger.info("Dropped " + msg + " from " + getSender().path().name());
                    return;
                }
            }

            if (!getRepository().existsContextWithId(msg.uuid)) {
                if (!(msg instanceof TxnBeginMsg)) {
                    logger.severe("Bad request: " + msg);
                    return;
                }
            }
        }

        super.aroundReceive(receive, obj);
    }

    @Override
    public Receive createReceive() {
        return new ReceiveBuilder()
                .match(JoinMessage.class, this::onJoinMessage)
                .match(StartMessage.class, this::onStartMsg)
                .matchAny(msg -> {
                    logger.info("Stashed " + msg + " from " + getSender().path().name());
                    stash();
                })
                .build();
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
                .match(Done.class, this::onDone)
                .match(TimeoutMsg.class, this::onTimeoutMsg)
                .match(Reset.class, this::onReset)
                .build();
    }

    private void onJoinMessage(JoinMessage msg) {
        IntStream.rangeClosed(msg.lowerKey, msg.upperKey).forEach(key -> dispatcher.map(key, getSender()));
    }

    private void onStartMsg(StartMessage msg) {
        if (dispatcher.getAll().isEmpty()) {
            logger.info("Starting. There is no available server.");
        } else {
            logger.info("Starting.\nAvailable servers: " + dispatcher.getAll().stream()
                    .map(server -> server.path().name())
                    .collect(Collectors.joining(", ")));
        }
        getContext().become(createAliveReceive());
        unstashAll();
    }

    private void onTimeoutMsg(TimeoutMsg timeout) {
        var ctx = getRepository().getRequestContextById(timeout.uuid);

        switch (ctx.loggedState()) {
            case CONVERSATIONAL:
            case START_2PC: {
                logger.info("Logged state is " + ctx.loggedState() + ", aborting the transaction");

                ctx.log(CoordinatorRequestContext.LogState.GLOBAL_ABORT);

                logger.info("Sending the final decision to the participants");
                var decision = new FinalDecision(ctx.uuid, FinalDecision.Decision.GLOBAL_ABORT);
                var multicast = Communication.builder()
                        .ofSender(getSelf())
                        .ofReceivers(ctx.getParticipants())
                        .ofMessage(decision)
                        .ofSuccessProbability(parameters.coordinatorOnFinalDecisionSuccessProbability);
                if (!multicast.run()) {
                    logger.info("Did not send the message to " + multicast.getMissing().stream()
                            .map(participant -> participant.path().name())
                            .collect(Collectors.joining(", ")));
                    crash();
                    return;
                }

                ctx.startTimer(this, CoordinatorRequestContext.DONE_TIMEOUT_S);

                logger.info("Sending the transaction result to " + ctx.subject.path().name());
                var result = new TxnResultMsg(ctx.uuid, false);
                ctx.subject.tell(result, getSelf());
                ctx.setCompleted();

                break;
            }
            case GLOBAL_COMMIT:
            case GLOBAL_ABORT: {
                logger.info("Logged state is " + ctx.loggedState());

                if (!ctx.allParticipantsDone()) {
                    logger.info("Soliciting the remaining participants");
                    var solicit = new Solicit(ctx.uuid);
                    ctx.getRemainingDoneParticipants().forEach(participant -> participant.tell(solicit, getSelf()));

                    ctx.startTimer(this, CoordinatorRequestContext.DONE_TIMEOUT_S);
                } else {
                    logger.info("All the participants are done, nothing to do");
                }

                break;
            }
        }
    }

    private void onTxnBeginMsg(TxnBeginMsg msg) {
        var ctx = new CoordinatorRequestContext(msg.uuid, getSender());
        getRepository().addRequestContext(ctx);

        ctx.log(CoordinatorRequestContext.LogState.CONVERSATIONAL);

        var accept = new TxnAcceptMsg(ctx.uuid);
        getSender().tell(accept, getSelf());

        ctx.startTimer(this, CoordinatorRequestContext.CONVERSATIONAL_TIMEOUT);
    }

    private void onTxnEndMsg(TxnEndMsg msg) {
        var ctx = getRepository().getRequestContextById(msg.uuid);

        switch (ctx.loggedState()) {
            case CONVERSATIONAL: {
                ctx.cancelTimer();
                if (msg.commit) {
                    logger.info(ctx.subject.path().name() + " requested to commit");

                    if (ctx.getParticipants().size() > 0) {
                        ctx.log(CoordinatorRequestContext.LogState.START_2PC);

                        logger.info("Asking the vote requests to the participants");
                        var request = new VoteRequest(msg.uuid, ctx.getParticipants());
                        var multicast = Communication.builder()
                                .ofSender(getSelf())
                                .ofReceivers(ctx.getParticipants())
                                .ofMessage(request)
                                .ofSuccessProbability(parameters.coordinatorOnVoteRequestSuccessProbability);
                        if (!multicast.run()) {
                            logger.info("Did not send the message to " + multicast.getMissing().stream()
                                    .map(participant -> participant.path().name())
                                    .collect(Collectors.joining(", ")));
                            crash();
                            return;
                        }

                        ctx.startTimer(this, CoordinatorRequestContext.VOTE_RESPONSE_TIMEOUT_S);
                    } else {
                        logger.info("No participant is involved");

                        ctx.log(CoordinatorRequestContext.LogState.GLOBAL_COMMIT);

                        logger.info("Sending the transaction result to " + ctx.subject.path().name());
                        var result = new TxnResultMsg(ctx.uuid, false);
                        ctx.subject.tell(result, getSelf());
                        ctx.setCompleted();
                    }
                } else {
                    logger.info(ctx.subject.path().name() + " requested to abort");

                    ctx.log(CoordinatorRequestContext.LogState.GLOBAL_ABORT);

                    if (ctx.getParticipants().size() > 0) {
                        logger.info("Sending the final decision to the participants");
                        var decision = new FinalDecision(ctx.uuid, FinalDecision.Decision.GLOBAL_ABORT);
                        var multicast = Communication.builder()
                                .ofSender(getSelf())
                                .ofReceivers(ctx.getParticipants())
                                .ofMessage(decision)
                                .ofSuccessProbability(parameters.coordinatorOnFinalDecisionSuccessProbability);
                        if (!multicast.run()) {
                            logger.info("Did not send the message to " + multicast.getMissing().stream()
                                    .map(participant -> participant.path().name())
                                    .collect(Collectors.joining(", ")));
                            crash();
                            return;
                        }

                        // if we received a client abort, we do not have to start the Two-phase commit (2PC) protocol,
                        // thus we must not start the vote response timer.
                        // Instead, we should wait for the Done messages to arrive in order to remove the transaction.
                        ctx.startTimer(this, CoordinatorRequestContext.DONE_TIMEOUT_S);
                    } else {
                        logger.info("No participant is involved");
                    }

                    logger.info("Sending the transaction result to " + ctx.subject.path().name());
                    var result = new TxnResultMsg(ctx.uuid, false);
                    ctx.subject.tell(result, getSelf());
                    ctx.setCompleted();
                }
                break;
            }
            case START_2PC: {
                // the coordinator is still running the Two-phase commit (2PC) protocol:
                // it cannot reply with a meaningful answer
                break;
            }
            case GLOBAL_COMMIT: {
                var result = new TxnResultMsg(ctx.uuid, true);
                getSender().tell(result, getSelf());
                ctx.setCompleted();
                break;
            }
            case GLOBAL_ABORT: {
                var result = new TxnResultMsg(ctx.uuid, false);
                getSender().tell(result, getSelf());
                ctx.setCompleted();
                break;
            }
        }
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
                            ctx.cancelTimer();

                            ctx.log(CoordinatorRequestContext.LogState.GLOBAL_COMMIT);

                            logger.info("All voted YES. Sending the final decision to the participants");
                            var decision = new FinalDecision(resp.uuid, FinalDecision.Decision.GLOBAL_COMMIT);
                            var multicast = Communication.builder()
                                    .ofSender(getSelf())
                                    .ofReceivers(ctx.getParticipants())
                                    .ofMessage(decision)
                                    .ofSuccessProbability(parameters.coordinatorOnFinalDecisionSuccessProbability);
                            if (!multicast.run()) {
                                logger.info("Did not send the message to " + multicast.getMissing().stream()
                                        .map(participant -> participant.path().name())
                                        .collect(Collectors.joining(", ")));
                                crash();
                                return;
                            }

                            ctx.startTimer(this, CoordinatorRequestContext.DONE_TIMEOUT_S);

                            logger.info("Sending the transaction result to " + ctx.subject.path().name());
                            var result = new TxnResultMsg(ctx.uuid, true);
                            ctx.subject.tell(result, getSelf());
                            ctx.setCompleted();
                        }
                        break;
                    }
                    case NO: {
                        logger.info("Received a NO vote from " + getSender().path().name());

                        ctx.cancelTimer();

                        ctx.log(CoordinatorRequestContext.LogState.GLOBAL_ABORT);

                        // The NO vote implicitly piggybacks the Done
                        ctx.addDoneParticipant(getSender());

                        logger.info("Sending the final decision to the participants");
                        var decision = new FinalDecision(resp.uuid, FinalDecision.Decision.GLOBAL_ABORT);
                        var multicast = Communication.builder()
                                .ofSender(getSelf())
                                .ofReceivers(ctx.getParticipants())
                                .ofMessage(decision)
                                .ofSuccessProbability(parameters.coordinatorOnFinalDecisionSuccessProbability);
                        if (!multicast.run()) {
                            logger.info("Did not send the message to " + multicast.getMissing().stream()
                                    .map(participant -> participant.path().name())
                                    .collect(Collectors.joining(", ")));
                            crash();
                            return;
                        }

                        ctx.startTimer(this, CoordinatorRequestContext.DONE_TIMEOUT_S);

                        logger.info("Sending the transaction result to " + ctx.subject.path().name());
                        var result = new TxnResultMsg(ctx.uuid, false);
                        ctx.subject.tell(result, getSelf());
                        ctx.setCompleted();

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
                logger.info("Received a VoteResponse, and the decision is already known");
            }
        }
    }

    private void onReadMsg(ReadMsg msg) {
        var ctx = getRepository().getRequestContextById(msg.uuid);

        if (ctx.loggedState() != CoordinatorRequestContext.LogState.CONVERSATIONAL) {
            logger.info("Arrived too late (" + ctx.loggedState() + ")");
            return;
        }

        ctx.cancelTimer();

        var server = dispatcher.getServer(msg.key);
        if (server.isEmpty()) {
            logger.info("No server for key " + msg.key + ". Aborting the transaction");
            ctx.log(CoordinatorRequestContext.LogState.GLOBAL_ABORT);

            if (ctx.getParticipants().size() > 0) {
                logger.info("Sending the final decision to the participants");
                var decision = new FinalDecision(ctx.uuid, FinalDecision.Decision.GLOBAL_ABORT);
                var multicast = Communication.builder()
                        .ofSender(getSelf())
                        .ofReceivers(ctx.getParticipants())
                        .ofMessage(decision)
                        .ofSuccessProbability(getParameters().coordinatorOnFinalDecisionSuccessProbability);
                if (!multicast.run()) {
                    logger.info("Did not send the message to " + multicast.getMissing().stream()
                            .map(participant -> participant.path().name())
                            .collect(Collectors.joining(", ")));
                    crash();
                    return;
                }

                ctx.startTimer(this, CoordinatorRequestContext.DONE_TIMEOUT_S);
            }

            logger.info("Sending the transaction result to " + ctx.subject.path().name());
            var result = new TxnResultMsg(ctx.uuid, false);
            getSender().tell(result, getSelf());
            ctx.setCompleted();
        } else {
            ctx.addParticipant(server.get());

            var req = new ReadRequest(msg.uuid, msg.key);
            server.get().tell(req, getSelf());

            ctx.startTimer(this, CoordinatorRequestContext.CONVERSATIONAL_TIMEOUT);
        }
    }

    private void onReadResult(ReadResult msg) {
        var ctx = getRepository().getRequestContextById(msg.uuid);

        if (ctx.loggedState() != CoordinatorRequestContext.LogState.CONVERSATIONAL) {
            logger.info("Arrived too late: logged state (" + ctx.loggedState() + ")");
            return;
        }

        ctx.cancelTimer();

        var result = new ReadResultMsg(msg.uuid, msg.key, msg.value);
        ctx.subject.tell(result, getSelf());

        // give the client more time, as it may be blocked, waiting to receive the read result
        ctx.startTimer(this, CoordinatorRequestContext.CONVERSATIONAL_TIMEOUT);
    }

    private void onWriteMsg(WriteMsg msg) {
        var ctx = getRepository().getRequestContextById(msg.uuid);

        if (ctx.loggedState() != CoordinatorRequestContext.LogState.CONVERSATIONAL) {
            logger.info("Arrived too late (" + ctx.loggedState() + ")");
            return;
        }

        ctx.cancelTimer();

        var server = dispatcher.getServer(msg.key);
        if (server.isEmpty()) {
            logger.info("No server for key " + msg.key + ". Aborting the transaction");
            ctx.log(CoordinatorRequestContext.LogState.GLOBAL_ABORT);

            if (ctx.getParticipants().size() > 0) {
                logger.info("Sending the final decision to the participants");
                var decision = new FinalDecision(ctx.uuid, FinalDecision.Decision.GLOBAL_ABORT);
                var multicast = Communication.builder()
                        .ofSender(getSelf())
                        .ofReceivers(ctx.getParticipants())
                        .ofMessage(decision)
                        .ofSuccessProbability(getParameters().coordinatorOnFinalDecisionSuccessProbability);
                if (!multicast.run()) {
                    logger.info("Did not send the message to " + multicast.getMissing().stream()
                            .map(participant -> participant.path().name())
                            .collect(Collectors.joining(", ")));
                    crash();
                    return;
                }

                ctx.startTimer(this, CoordinatorRequestContext.DONE_TIMEOUT_S);
            }

            logger.info("Sending the transaction result to " + ctx.subject.path().name());
            var result = new TxnResultMsg(ctx.uuid, false);
            getSender().tell(result, getSelf());
            ctx.setCompleted();
        } else {
            ctx.addParticipant(server.get());

            var req = new WriteRequest(msg.uuid, msg.key, msg.value);
            server.get().tell(req, getSelf());

            // give the client more time
            ctx.startTimer(this, CoordinatorRequestContext.CONVERSATIONAL_TIMEOUT);
        }
    }

    private void onDone(Done msg) {
        var ctx = getRepository().getRequestContextById(msg.uuid);

        // all logged states are possible.
        // CONVERSATIONAL:  when the coordinator has not yet received the TxnEndMsg from the client;
        //                  a crashed participant resumes, aborts and sends Done.
        // START_2PC:       when the coordinator sends the VoteRequest;
        //                  a crashed participant drops it, resumes, aborts and sends Done.

        ctx.addDoneParticipant(getSender());

        if (ctx.allParticipantsDone()) {
            logger.info("All the participants are done");
            ctx.cancelTimer();
        } else {
            var missing = ctx.getRemainingDoneParticipants();
            logger.info(missing.size() + " Done messages required left, from " + missing.stream()
                    .map(participant -> participant.path().name())
                    .collect(Collectors.joining(", ")));
        }
    }

    private void onReset(Reset reset) {
        var ctx = getRepository().getRequestContextById(reset.uuid);

        switch (ctx.loggedState()) {
            case CONVERSATIONAL: {
                logger.severe("Logged state is CONVERSATIONAL, should be START_2PC or GLOBAL_ABORT");
                return;
            }
            case START_2PC: {
                logger.info("Logged state is START_2PC: removing " + getSender().path().name() + " from the participants");
                ctx.removeParticipant(getSender());

                ctx.log(CoordinatorRequestContext.LogState.GLOBAL_ABORT);

                // we are not any more interested in collecting the votes, as we now know the final decision
                ctx.cancelTimer();

                if (!ctx.allParticipantsDone()) {
                    logger.info("Sending the final decision to the participants");
                    var decision = new FinalDecision(ctx.uuid, FinalDecision.Decision.GLOBAL_ABORT);
                    var multicast = Communication.builder()
                            .ofSender(getSelf())
                            .ofReceivers(ctx.getParticipants())
                            .ofMessage(decision)
                            .ofSuccessProbability(parameters.coordinatorOnFinalDecisionSuccessProbability);
                    if (!multicast.run()) {
                        logger.info("Did not send the message to " + multicast.getMissing().stream()
                                .map(participant -> participant.path().name())
                                .collect(Collectors.joining(", ")));
                        crash();
                        return;
                    }

                    ctx.startTimer(this, CoordinatorRequestContext.DONE_TIMEOUT_S);
                }
                //else {
                // the participant we just removed was the only one participating in this transaction
                //}

                var result = new TxnResultMsg(ctx.uuid, false);
                ctx.subject.tell(result, getSelf());

                break;
            }
            case GLOBAL_COMMIT: {
                logger.severe("Logged state is GLOBAL_COMMIT, should be START_2PC or GLOBAL_ABORT");
                return;
            }
            case GLOBAL_ABORT: {
                logger.info("Logged state is GLOBAL_ABORT: removing " + getSender().path().name() + " from the participants");
                ctx.removeParticipant(getSender());

                if (ctx.allParticipantsDone()) {
                    ctx.cancelTimer();
                }

                break;
            }
        }
    }

    @Override
    protected void crash() {
        super.crash();

        getRepository().getAllRequestContexts().stream()
                .filter(ctx -> !ctx.isCompleted() || !ctx.allParticipantsDone())
                .forEach(ctx -> {
                    logger.info("Crashing transaction " + ctx.uuid);

                    ctx.cancelTimer();
                });

        if (parameters.coordinatorCanRecover) {
            getContext().system().scheduler().scheduleOnce(
                    Duration.ofMillis(parameters.coordinatorRecoveryTimeMs), // delay
                    getSelf(), // receiver
                    new ResumeMessage(), // message
                    getContext().dispatcher(), // executor
                    ActorRef.noSender()); // sender
        }
    }

    @Override
    public void resume() {
        super.resume();

        getRepository().getAllRequestContexts().stream().filter(ctx -> !(ctx.isCompleted() && ctx.allParticipantsDone())).forEach(ctx -> {
            logger.info("Resuming transaction " + ctx.uuid);

            switch (ctx.loggedState()) {
                case CONVERSATIONAL: {
                    // Bernstein, p. 231, case 1
                    logger.info("Logged state is CONVERSATIONAL: aborting the transaction");
                    ctx.log(CoordinatorRequestContext.LogState.GLOBAL_ABORT);

                    ctx.startTimer(this, CoordinatorRequestContext.DONE_TIMEOUT_S);

                    if (!ctx.isCompleted()) {
                        logger.info("Sending the transaction result to " + ctx.subject.path().name());
                        var result = new TxnResultMsg(ctx.uuid, false);
                        ctx.subject.tell(result, getSelf());
                        ctx.setCompleted();
                    }

                    break;
                }
                case START_2PC: {
                    // Bernstein, p. 231, case 2
                    logger.info("Logged state is START_2PC: aborting the transaction");
                    ctx.log(CoordinatorRequestContext.LogState.GLOBAL_ABORT);

                    var decision = new FinalDecision(ctx.uuid, FinalDecision.Decision.GLOBAL_ABORT);
                    var multicast = Communication.builder()
                            .ofSender(getSelf())
                            .ofReceivers(ctx.getParticipants())
                            .ofMessage(decision)
                            .ofSuccessProbability(parameters.coordinatorOnFinalDecisionSuccessProbability);
                    if (!multicast.run()) {
                        logger.info("Did not send the message to " + multicast.getMissing().stream()
                                .map(participant -> participant.path().name())
                                .collect(Collectors.joining(", ")));
                        crash();
                        return;
                    }

                    ctx.startTimer(this, CoordinatorRequestContext.DONE_TIMEOUT_S);

                    break;
                }
                case GLOBAL_COMMIT: {
                    if (!ctx.allParticipantsDone()) {
                        // Bernstein, p. 231, case 3
                        logger.info("Logged state is GLOBAL_COMMIT: retransmitting the final decision to the participants");

                        var decision = new FinalDecision(ctx.uuid, FinalDecision.Decision.GLOBAL_COMMIT);
                        var multicast = Communication.builder()
                                .ofSender(getSelf())
                                .ofReceivers(ctx.getParticipants())
                                .ofMessage(decision)
                                .ofSuccessProbability(parameters.coordinatorOnFinalDecisionSuccessProbability);
                        if (!multicast.run()) {
                            logger.info("Did not send the message to " + multicast.getMissing().stream()
                                    .map(participant -> participant.path().name())
                                    .collect(Collectors.joining(", ")));
                            crash();
                            return;
                        }

                        ctx.startTimer(this, CoordinatorRequestContext.DONE_TIMEOUT_S);
                    }

                    if (!ctx.isCompleted()) {
                        logger.info("Sending the transaction result to " + ctx.subject.path().name());
                        var result = new TxnResultMsg(ctx.uuid, true);
                        ctx.subject.tell(result, getSelf());
                        ctx.setCompleted();
                    }

                    break;
                }
                case GLOBAL_ABORT: {
                    if (!ctx.allParticipantsDone()) {
                        // Bernstein, p. 231, case 3
                        logger.info("Logged state is GLOBAL_ABORT: retransmitting the final decision to the participants");

                        var decision = new FinalDecision(ctx.uuid, FinalDecision.Decision.GLOBAL_ABORT);
                        var multicast = Communication.builder()
                                .ofSender(getSelf())
                                .ofReceivers(ctx.getParticipants())
                                .ofMessage(decision)
                                .ofSuccessProbability(parameters.coordinatorOnFinalDecisionSuccessProbability);
                        if (!multicast.run()) {
                            logger.info("Did not send the message to " + multicast.getMissing().stream()
                                    .map(participant -> participant.path().name())
                                    .collect(Collectors.joining(", ")));
                            crash();
                            return;
                        }

                        ctx.startTimer(this, CoordinatorRequestContext.DONE_TIMEOUT_S);
                    }

                    if (!ctx.isCompleted()) {
                        logger.info("Sending the transaction result to " + ctx.subject.path().name());
                        var result = new TxnResultMsg(ctx.uuid, false);
                        ctx.subject.tell(result, getSelf());
                        ctx.setCompleted();
                    }

                    break;
                }
            }
        });
    }
}
