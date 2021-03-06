package it.unitn.arpino.ds1project.nodes.server;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.datastore.controller.IDatabaseController;
import it.unitn.arpino.ds1project.datastore.database.DatabaseBuilder;
import it.unitn.arpino.ds1project.datastore.database.IDatabase;
import it.unitn.arpino.ds1project.messages.ResumeMessage;
import it.unitn.arpino.ds1project.messages.TimeoutMsg;
import it.unitn.arpino.ds1project.messages.TxnMessage;
import it.unitn.arpino.ds1project.messages.coordinator.Done;
import it.unitn.arpino.ds1project.messages.coordinator.ReadResult;
import it.unitn.arpino.ds1project.messages.coordinator.Reset;
import it.unitn.arpino.ds1project.messages.coordinator.VoteResponse;
import it.unitn.arpino.ds1project.messages.server.*;
import it.unitn.arpino.ds1project.nodes.DataStoreNode;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.simulation.Communication;
import it.unitn.arpino.ds1project.simulation.ServerParameters;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.time.Duration;
import java.util.stream.Collectors;

public class Server extends DataStoreNode<ServerRequestContext> {
    private final ServerParameters parameters;

    private final IDatabase database;
    private final IDatabaseController controller;

    public Server(int lowerKey, int upperKey) {
        parameters = new ServerParameters();

        DatabaseBuilder builder = DatabaseBuilder.newBuilder()
                .keyRange(lowerKey, upperKey)
                .create();
        database = builder.getDatabase();
        controller = builder.getController();
    }

    public static Props props(int lowerKey, int upperKey) {
        return Props.create(Server.class, () -> new Server(lowerKey, upperKey)).withDispatcher("my-pinned-dispatcher");
    }

    public ServerParameters getParameters() {
        return parameters;
    }

    public IDatabase getDatabase() {
        return database;
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
                if (!(msg instanceof ReadRequest) && !(msg instanceof WriteRequest)) {
                    logger.info("Bad request, sending Reset");

                    var reset = new Reset(msg.uuid);
                    getSender().tell(reset, getSelf());

                    return;
                }
            }
        }

        super.aroundReceive(receive, obj);
    }

    @Override
    public Receive createReceive() {
        return createAliveReceive();
    }

    @Override
    protected Receive createAliveReceive() {
        return new ReceiveBuilder()
                .match(ReadRequest.class, this::onReadRequest)
                .match(WriteRequest.class, this::onWriteRequest)
                .match(VoteRequest.class, this::onVoteRequest)
                .match(FinalDecision.class, this::onFinalDecision)
                .match(TimeoutMsg.class, this::onTimeoutMsg)
                .match(DecisionResponse.class, this::onDecisionResponse)
                .match(DecisionRequest.class, this::onDecisionRequest)
                .match(Solicit.class, this::onSolicit)
                .build();
    }


    private void onReadRequest(ReadRequest req) {
        if (!getRepository().existsContextWithId(req.uuid)) {
            var connection = controller.beginTransaction();
            var ctx = new ServerRequestContext(req.uuid, getSender(), connection);
            getRepository().addRequestContext(ctx);

            ctx.log(ServerRequestContext.LogState.CONVERSATIONAL);
        }

        var ctx = getRepository().getRequestContextById(req.uuid);

        if (ctx.loggedState() != ServerRequestContext.LogState.CONVERSATIONAL) {
            logger.severe("Invalid protocol state (" + ctx.loggedState() + ", should be CONVERSATIONAL)");
            return;
        }

        ctx.startTimer(this, ServerRequestContext.CONVERSATIONAL_TIMEOUT);

        int value = ctx.read(req.key);

        var result = new ReadResult(req.uuid, req.key, value);
        getSender().tell(result, getSelf());
    }

    private void onWriteRequest(WriteRequest req) {
        if (!getRepository().existsContextWithId(req.uuid)) {
            var connection = controller.beginTransaction();
            var ctx = new ServerRequestContext(req.uuid, getSender(), connection);
            getRepository().addRequestContext(ctx);

            ctx.log(ServerRequestContext.LogState.CONVERSATIONAL);
        }

        var ctx = getRepository().getRequestContextById(req.uuid);

        if (ctx.loggedState() != ServerRequestContext.LogState.CONVERSATIONAL) {
            logger.severe("Invalid protocol state (" + ctx.loggedState() + ", should be CONVERSATIONAL)");
            return;
        }

        ctx.startTimer(this, ServerRequestContext.CONVERSATIONAL_TIMEOUT);

        ctx.write(req.key, req.value);
    }

    private void onVoteRequest(VoteRequest req) {
        var ctx = getRepository().getRequestContextById(req.uuid);

        if (ctx.loggedState() != ServerRequestContext.LogState.CONVERSATIONAL) {
            logger.info("Arrived too late (" + ctx.loggedState() + ")");
            return;
        }

        ctx.cancelTimer();

        req.participants.stream()
                .filter(participant -> !participant.equals(getSelf()))
                .forEach(ctx::addParticipant);

        if (ctx.prepare()) {
            logger.info("Transaction prepared: voting commit");

            ctx.log(ServerRequestContext.LogState.VOTE_COMMIT);

            var yesVote = new VoteResponse(req.uuid, VoteResponse.Vote.YES);
            var unicast = Communication.builder()
                    .ofSender(getSelf())
                    .ofReceiver(getSender())
                    .ofMessage(yesVote)
                    .ofSuccessProbability(parameters.serverOnVoteResponseSuccessProbability);
            if (!unicast.run()) {
                logger.info("Did not send the message to " + getSender().path().name());
                crash();
                return;
            }

            ctx.startTimer(this, ServerRequestContext.FINAL_DECISION_TIMEOUT_S);
        } else {
            logger.info("Aborting the transaction");

            ctx.log(ServerRequestContext.LogState.GLOBAL_ABORT);

            // The NO vote implicitly piggybacks the Done
            var noVote = new VoteResponse(req.uuid, VoteResponse.Vote.NO);
            var unicast = Communication.builder()
                    .ofSender(getSelf())
                    .ofReceiver(getSender())
                    .ofMessage(noVote)
                    .ofSuccessProbability(parameters.serverOnVoteResponseSuccessProbability);
            if (!unicast.run()) {
                logger.info("Did not send the message to " + getSender().path().name());
                crash();
            }

            // if we could not prepare the transaction and have aborted, we do not have to wait for a final decision,
            // thus we must not start the final decision timer.
        }
    }

    private void onTimeoutMsg(TimeoutMsg timeout) {
        var ctx = getRepository().getRequestContextById(timeout.uuid);

        switch (ctx.loggedState()) {
            case CONVERSATIONAL: {
                logger.info("Logged state is CONVERSATIONAL, aborting the transaction");

                ctx.log(ServerRequestContext.LogState.GLOBAL_ABORT);
                ctx.abort();

                logger.info("Sending a Done message");
                var done = new Done(ctx.uuid);
                ctx.subject.tell(done, getSelf());

                break;
            }
            case VOTE_COMMIT: {
                logger.info("Logged state is VOTE_COMMIT, starting the termination protocol");
                terminationProtocol(ctx);
                break;
            }
            case GLOBAL_COMMIT: {
                // example: the timer expired, the server sends itself a TimeoutMsg, which goes at the end of the
                // message queue. The next message the server processes makes the server learn the final decision
                // and transition to this state.
                logger.info("The decision is already known (GLOBAL_COMMIT)");
                break;
            }
            case GLOBAL_ABORT: {
                // same as above
                logger.info("The decision is already known (GLOBAL_ABORT)");
                break;
            }
        }
    }

    /**
     * A server which is executing the termination protocol is requesting to this server the final decision of a transaction.
     */
    private void onDecisionRequest(DecisionRequest req) {
        var ctx = getRepository().getRequestContextById(req.uuid);

        DecisionResponse response = null;

        switch (ctx.loggedState()) {
            case CONVERSATIONAL: {
                // Tanenbaum, p. 486,
                logger.info("Logged state is CONVERSATIONAL: abort");

                ctx.cancelTimer();

                ctx.log(ServerRequestContext.LogState.GLOBAL_ABORT);
                ctx.abort();

                response = new DecisionResponse(ctx.uuid, DecisionResponse.Decision.GLOBAL_ABORT);
                break;
            }
            case VOTE_COMMIT: {
                response = new DecisionResponse(ctx.uuid, DecisionResponse.Decision.UNKNOWN);
                break;
            }
            case GLOBAL_COMMIT: {
                response = new DecisionResponse(ctx.uuid, DecisionResponse.Decision.GLOBAL_COMMIT);
                break;
            }
            case GLOBAL_ABORT: {
                response = new DecisionResponse(ctx.uuid, DecisionResponse.Decision.GLOBAL_ABORT);
                break;
            }
        }

        var unicast = Communication.builder()
                .ofSender(getSelf())
                .ofReceiver(getSender())
                .ofMessage(response)
                .ofSuccessProbability(parameters.serverOnDecisionResponseSuccessProbability);
        if (!unicast.run()) {
            logger.info("Did not send the message to " + getSender().path().name());
            crash();
        }
    }

    private void onDecisionResponse(DecisionResponse resp) {
        var ctx = getRepository().getRequestContextById(resp.uuid);

        switch (ctx.loggedState()) {
            case CONVERSATIONAL: {
                logger.severe("Invalid logged state (CONVERSATIONAL, should be VOTE_COMMIT, GLOBAL_ABORT or DECISION)");
                return;
            }
            case VOTE_COMMIT: {
                switch (resp.decision) {
                    case UNKNOWN: {
                        logger.info("Received UNKNOWN: ignoring");
                        break;
                    }
                    case GLOBAL_COMMIT: {
                        logger.info("Received GLOBAL_COMMIT: committing");
                        ctx.log(ServerRequestContext.LogState.GLOBAL_COMMIT);
                        ctx.commit();

                        // Whenever the server transitions to a state of certainty,
                        // it informs the coordinator that it is done.
                        logger.info("Sending a Done message");
                        var done = new Done(ctx.uuid);
                        ctx.subject.tell(done, getSelf());
                        break;
                    }
                    case GLOBAL_ABORT: {
                        logger.info("Received GLOBAL_ABORT: aborting");
                        ctx.log(ServerRequestContext.LogState.GLOBAL_ABORT);
                        ctx.abort();

                        // Whenever the server transitions to a state of certainty,
                        // it informs the coordinator that it is done.
                        logger.info("Sending a Done message");
                        var done = new Done(ctx.uuid);
                        ctx.subject.tell(done, getSelf());
                        break;
                    }
                }
                break;
            }
            case GLOBAL_COMMIT:
            case GLOBAL_ABORT: {
                logger.info("The decision is already known, ignoring");
                break;
            }
        }
    }

    /**
     * The {@link Coordinator} is sending the final decision to this participant.
     */
    private void onFinalDecision(FinalDecision req) {
        var ctx = getRepository().getRequestContextById(req.uuid);

        switch (ctx.loggedState()) {
            case CONVERSATIONAL: {
                if (req.decision != FinalDecision.Decision.GLOBAL_ABORT) {
                    logger.severe("Received GLOBAL_COMMIT but the logged state is CONVERSATIONAL");
                    return;
                }

                logger.info("Received while in INIT: client abort or coordinator timeout");

                ctx.cancelTimer();

                ctx.log(ServerRequestContext.LogState.GLOBAL_ABORT);
                ctx.abort();

                break;
            }
            case VOTE_COMMIT: {
                // This line used to cause a null pointer exception:
                // There is a case in which the final decision timer was never started.
                // It happens when, in the function onVoteRequest, in the true branch of branch ctx.prepare(),
                // the server crashes and thus does not reach the line with ctx.startFinalDecisionTimer(this).
                ctx.cancelTimer();

                switch (req.decision) {
                    case GLOBAL_COMMIT: {
                        logger.info("Received GLOBAL_COMMIT: committing");
                        ctx.log(ServerRequestContext.LogState.GLOBAL_COMMIT);
                        ctx.commit();
                        break;
                    }
                    case GLOBAL_ABORT: {
                        logger.info("Received GLOBAL_ABORT: aborting");
                        ctx.log(ServerRequestContext.LogState.GLOBAL_ABORT);
                        ctx.abort();
                        break;
                    }
                }

                break;
            }
            case GLOBAL_COMMIT: {
                logger.info("This should be a retransmission");
                break;
            }
            case GLOBAL_ABORT: {
                if (req.decision != FinalDecision.Decision.GLOBAL_ABORT) {
                    logger.severe("Received GLOBAL_COMMIT but the logged state is GLOBAL_ABORT");
                    return;
                }
                logger.info("This should be a retransmission");

                break;
            }
        }

        // always send the Done message, regardless of logged state
        logger.info("Sending a Done message");
        var done = new Done(ctx.uuid);
        ctx.subject.tell(done, getSelf());
    }

    /**
     * The server asks every other server for the {@link FinalDecision} with a {@link DecisionRequest} and remains
     * blocked until it receives a response.
     *
     * @param ctx Context for which to start the termination protocol
     */
    private void terminationProtocol(ServerRequestContext ctx) {
        var request = new DecisionRequest(ctx.uuid);
        var multicast = Communication.builder()
                .ofSender(getSelf())
                .ofReceivers(ctx.getParticipants())
                .ofMessage(request)
                .ofSuccessProbability(parameters.serverOnDecisionRequestSuccessProbability);
        if (!multicast.run()) {
            logger.info("Did not send the message to " + multicast.getMissing().stream()
                    .map(participant -> participant.path().name())
                    .collect(Collectors.joining(", ")));
            crash();
        }
    }

    private void onSolicit(Solicit solicit) {
        var ctx = getRepository().getRequestContextById(solicit.uuid);

        switch (ctx.loggedState()) {
            case CONVERSATIONAL: {
                logger.severe("Invalid protocol state (CONVERSATIONAL, should be VOTE_COMMIT, GLOBAL_ABORT or DECISION)");
                return;
            }
            case VOTE_COMMIT: {
                logger.info("The decision is not known: starting the termination protocol");
                terminationProtocol(ctx);
                break;
            }
            case GLOBAL_COMMIT:
            case GLOBAL_ABORT: {
                logger.info("Sending a Done message");
                var done = new Done(ctx.uuid);
                getSender().tell(done, getSelf());
                break;
            }
        }
    }

    @Override
    protected void crash() {
        super.crash();

        getRepository().getAllRequestContexts().forEach(ServerRequestContext::cancelTimer);

        if (parameters.serverCanRecover) {
            getContext().system().scheduler().scheduleOnce(
                    Duration.ofMillis(parameters.serverRecoveryTimeMs), // delay
                    getSelf(), // receiver
                    new ResumeMessage(), // message
                    getContext().dispatcher(), // executor
                    ActorRef.noSender()); // sender
        }
    }

    @Override
    protected void resume() {
        super.resume();

        getRepository().getAllRequestContexts().stream().filter(ctx -> !ctx.isDecided()).forEach(ctx -> {
            logger.info("Resuming transaction " + ctx.uuid);

            switch (ctx.loggedState()) {
                case CONVERSATIONAL: {
                    // If the server has not already cast the vote for the transaction, it aborts it.
                    logger.info("Aborting transaction " + ctx.uuid);

                    ctx.log(ServerRequestContext.LogState.GLOBAL_ABORT);
                    ctx.abort();

                    logger.info("Sending a Done message");
                    var done = new Done(ctx.uuid);
                    ctx.subject.tell(done, getSelf());

                    break;
                }
                case VOTE_COMMIT: {
                    // If the server has already cast the vote for the transaction, it asks the others about the coordinator's
                    // FinalDecision.
                    logger.info("Starting the termination protocol for transaction " + ctx.uuid);

                    terminationProtocol(ctx);
                    break;
                }
            }
        });
    }
}
