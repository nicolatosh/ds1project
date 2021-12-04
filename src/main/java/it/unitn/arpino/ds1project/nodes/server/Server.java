package it.unitn.arpino.ds1project.nodes.server;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.datastore.controller.IDatabaseController;
import it.unitn.arpino.ds1project.datastore.database.DatabaseBuilder;
import it.unitn.arpino.ds1project.datastore.database.IDatabase;
import it.unitn.arpino.ds1project.messages.JoinMessage;
import it.unitn.arpino.ds1project.messages.ResumeMessage;
import it.unitn.arpino.ds1project.messages.TxnMessage;
import it.unitn.arpino.ds1project.messages.coordinator.Done;
import it.unitn.arpino.ds1project.messages.coordinator.ReadResult;
import it.unitn.arpino.ds1project.messages.coordinator.Reset;
import it.unitn.arpino.ds1project.messages.coordinator.VoteResponse;
import it.unitn.arpino.ds1project.messages.server.*;
import it.unitn.arpino.ds1project.nodes.DataStoreNode;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.ServerRequestContext.TwoPhaseCommitFSM;
import it.unitn.arpino.ds1project.simulation.Communication;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Server extends DataStoreNode<ServerRequestContext> {
    private final IDatabase database;
    private final IDatabaseController controller;

    /**
     * The other servers in the Data Store that this server can contact in the Two-phase commit (2PC) termination protocol
     */
    private final List<ActorRef> servers;

    public Server(int lowerKey, int upperKey) {
        DatabaseBuilder builder = DatabaseBuilder.newBuilder()
                .keyRange(lowerKey, upperKey)
                .create();
        database = builder.getDatabase();
        controller = builder.getController();
        servers = new ArrayList<>();
    }

    public static Props props(int lowerKey, int upperKey) {
        return Props.create(Server.class, () -> new Server(lowerKey, upperKey));
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
                    logger.severe("Bad request");

                    var reset = new Reset(msg.uuid);
                    getSender().tell(reset, getSelf());

                    return;
                }
            }
        }

        super.aroundReceive(receive, obj);
    }

    @Override
    protected Receive createAliveReceive() {
        return new ReceiveBuilder()
                .match(ReadRequest.class, this::onReadRequest)
                .match(WriteRequest.class, this::onWriteRequest)
                .match(VoteRequest.class, this::onVoteRequest)
                .match(FinalDecision.class, this::onFinalDecision)
                .match(VoteRequestTimeout.class, this::onVoteRequestTimeout)
                .match(FinalDecisionTimeout.class, this::onFinalDecisionTimeout)
                .match(DecisionResponse.class, this::onDecisionResponse)
                .match(DecisionRequest.class, this::onDecisionRequest)
                .match(Solicit.class, this::onSolicit)
                .build();
    }

    @Override
    protected void onJoinMessage(JoinMessage msg) {
        logger.info(getSender().path().name() + " joined");
        servers.add(getSender());
    }


    private void onReadRequest(ReadRequest req) {
        if (!getRepository().existsContextWithId(req.uuid)) {
            var connection = controller.beginTransaction();
            var ctx = new ServerRequestContext(req.uuid, getSender(), connection);
            getRepository().addRequestContext(ctx);

            ctx.log(ServerRequestContext.LogState.INIT);
            ctx.setProtocolState(TwoPhaseCommitFSM.INIT);
            ctx.startVoteRequestTimer(this);
        }

        var ctx = getRepository().getRequestContextById(req.uuid);

        if (ctx.loggedState() != ServerRequestContext.LogState.INIT) {
            logger.severe("Invalid protocol state (" + ctx.loggedState() + ", should be INIT)");
            return;
        }

        int value = ctx.read(req.key);

        var result = new ReadResult(req.uuid, req.key, value);
        getSender().tell(result, getSelf());
    }

    private void onWriteRequest(WriteRequest req) {
        if (!getRepository().existsContextWithId(req.uuid)) {
            var connection = controller.beginTransaction();
            var ctx = new ServerRequestContext(req.uuid, getSender(), connection);
            getRepository().addRequestContext(ctx);

            ctx.log(ServerRequestContext.LogState.INIT);
            ctx.setProtocolState(TwoPhaseCommitFSM.INIT);
            ctx.startVoteRequestTimer(this);
        }

        var ctx = getRepository().getRequestContextById(req.uuid);

        if (ctx.loggedState() != ServerRequestContext.LogState.INIT) {
            logger.severe("Invalid protocol state (" + ctx.loggedState() + ", should be INIT)");
            return;
        }

        ctx.write(req.key, req.value);
    }

    private void onVoteRequest(VoteRequest req) {
        var ctx = getRepository().getRequestContextById(req.uuid);

        if (ctx.loggedState() != ServerRequestContext.LogState.INIT) {
            logger.severe("Invalid logged state (" + ctx.loggedState() + ", should be INIT)");
            return;
        }

        ctx.cancelVoteRequestTimer();

        if (ctx.prepare()) {
            logger.info("Transaction prepared: voting commit");

            ctx.log(ServerRequestContext.LogState.VOTE_COMMIT);

            var yesVote = new VoteResponse(req.uuid, VoteResponse.Vote.YES);
            var unicast = Communication.builder()
                    .ofSender(getSelf())
                    .ofReceiver(getSender())
                    .ofMessage(yesVote)
                    .ofCrashProbability(getParameters().serverOnVoteResponseCrashProbability);
            if (!unicast.run()) {
                logger.info("Did not send the message to " + getSender().path().name());
                crash();
                return;
            }

            ctx.setProtocolState(TwoPhaseCommitFSM.READY);

            ctx.startFinalDecisionTimer(this);
        } else {
            logger.info("Aborting the transaction");

            ctx.log(ServerRequestContext.LogState.GLOBAL_ABORT);

            // The NO vote implicitly piggybacks the Done
            var noVote = new VoteResponse(req.uuid, VoteResponse.Vote.NO);
            var unicast = Communication.builder()
                    .ofSender(getSelf())
                    .ofReceiver(getSender())
                    .ofMessage(noVote)
                    .ofCrashProbability(getParameters().serverOnVoteResponseCrashProbability);
            if (!unicast.run()) {
                logger.info("Did not send the message to " + getSender().path().name());
                crash();
                return;
            }

            ctx.setProtocolState(TwoPhaseCommitFSM.ABORT);

            // if we could not prepare the transaction and have aborted, we do not have to wait for a final decision,
            // thus we must not start the final decision timer.
        }
    }

    private void onVoteRequestTimeout(VoteRequestTimeout timeout) {
        var ctx = getRepository().getRequestContextById(timeout.uuid);

        if (ctx.loggedState() != ServerRequestContext.LogState.INIT) {
            logger.severe("Invalid logged state (" + ctx.loggedState() + ", should be INIT)");
            return;
        }

        logger.info("Aborting the transaction");

        ctx.log(ServerRequestContext.LogState.GLOBAL_ABORT);
        ctx.abort();
        ctx.setProtocolState(TwoPhaseCommitFSM.ABORT);

        logger.info("Sending a Done message");
        var done = new Done(ctx.uuid);
        ctx.subject.tell(done, getSelf());
    }

    private void onFinalDecisionTimeout(FinalDecisionTimeout timeout) {
        var ctx = getRepository().getRequestContextById(timeout.uuid);

        if (ctx.loggedState() != ServerRequestContext.LogState.VOTE_COMMIT) {
            logger.severe("Invalid logged state (" + ctx.loggedState() + ", should be VOTE_COMMIT)");
            return;
        }

        logger.info("Starting the termination protocol");
        terminationProtocol(ctx);
    }

    /**
     * A server which is executing the termination protocol is requesting to this server the final decision of a transaction.
     * If this server is not participating in that transaction, it ignores the request.
     */
    private void onDecisionRequest(DecisionRequest req) {
        if (!getRepository().existsContextWithId(req.uuid)) {
            logger.info("This server is not participating in the transaction: ignoring");
            return;
        }

        var ctx = getRepository().getRequestContextById(req.uuid);

        DecisionResponse response = null;

        switch (ctx.loggedState()) {
            case INIT: {
                // Tanenbaum, p. 486,
                logger.info("Logged state is INIT: abort");

                ctx.cancelVoteRequestTimer();

                ctx.log(ServerRequestContext.LogState.GLOBAL_ABORT);
                ctx.abort();
                ctx.setProtocolState(TwoPhaseCommitFSM.ABORT);

                response = new DecisionResponse(ctx.uuid, DecisionResponse.Decision.GLOBAL_ABORT);
                break;
            }
            case VOTE_COMMIT: {
                response = new DecisionResponse(ctx.uuid, DecisionResponse.Decision.UNKNOWN);
                break;
            }
            case DECISION: {
                switch (ctx.getProtocolState()) {
                    case ABORT: {
                        response = new DecisionResponse(ctx.uuid, DecisionResponse.Decision.GLOBAL_ABORT);
                        break;
                    }
                    case COMMIT: {
                        response = new DecisionResponse(ctx.uuid, DecisionResponse.Decision.GLOBAL_COMMIT);
                        break;
                    }
                }
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
                .ofCrashProbability(getParameters().serverOnDecisionResponseCrashProbability);
        if (!unicast.run()) {
            logger.info("Did not send the message to " + getSender().path().name());
            crash();
        }
    }

    private void onDecisionResponse(DecisionResponse resp) {
        var ctx = getRepository().getRequestContextById(resp.uuid);

        switch (ctx.loggedState()) {
            case INIT: {
                logger.severe("Invalid logged state (INIT, should be VOTE_COMMIT, GLOBAL_ABORT or DECISION)");
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
                        ctx.log(ServerRequestContext.LogState.DECISION);
                        ctx.commit();
                        ctx.setProtocolState(TwoPhaseCommitFSM.COMMIT);

                        // Whenever the server transitions to a state of certainty,
                        // it informs the coordinator that it is done.
                        logger.info("Sending a Done message");
                        var done = new Done(ctx.uuid);
                        ctx.subject.tell(done, getSelf());
                        break;
                    }
                    case GLOBAL_ABORT: {
                        logger.info("Received GLOBAL_ABORT: aborting");
                        ctx.log(ServerRequestContext.LogState.DECISION);
                        ctx.abort();
                        ctx.setProtocolState(TwoPhaseCommitFSM.ABORT);

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
            case GLOBAL_ABORT:
            case DECISION: {
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
            case INIT: {
                if (req.decision != FinalDecision.Decision.GLOBAL_ABORT) {
                    logger.severe("Received GLOBAL_COMMIT but the logged state is INIT");
                    return;
                }

                logger.info("Received while in INIT: client abort or coordinator timeout");

                ctx.cancelVoteRequestTimer();

                ctx.log(ServerRequestContext.LogState.DECISION);
                ctx.abort();
                ctx.setProtocolState(TwoPhaseCommitFSM.ABORT);

                break;
            }
            case VOTE_COMMIT: {
                // This line used to cause a null pointer exception:
                // There is a case in which the final decision timer was never started.
                // It happens when, in the function onVoteRequest, in the true branch of branch ctx.prepare(),
                // the server crashes and thus does not reach the line with ctx.startFinalDecisionTimer(this).
                ctx.cancelFinalDecisionTimer();

                switch (req.decision) {
                    case GLOBAL_COMMIT: {
                        logger.info("Received GLOBAL_COMMIT: committing");
                        ctx.log(ServerRequestContext.LogState.DECISION);
                        ctx.commit();
                        ctx.setProtocolState(TwoPhaseCommitFSM.COMMIT);
                        break;
                    }
                    case GLOBAL_ABORT: {
                        logger.info("Received GLOBAL_ABORT: aborting");
                        ctx.log(ServerRequestContext.LogState.DECISION);
                        ctx.abort();
                        ctx.setProtocolState(TwoPhaseCommitFSM.ABORT);
                        break;
                    }
                }

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
            case DECISION: {
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
                .ofReceivers(servers)
                .ofMessage(request)
                .ofCrashProbability(getParameters().serverOnDecisionRequestCrashProbability);
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
            case INIT: {
                logger.severe("Invalid protocol state (INIT, should be VOTE_COMMIT, GLOBAL_ABORT or DECISION)");
                return;
            }
            case VOTE_COMMIT: {
                logger.info("The decision is not known: starting the termination protocol");
                terminationProtocol(ctx);
                break;
            }
            case GLOBAL_ABORT:
            case DECISION: {
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

        getRepository().getAllRequestContexts().forEach(ServerRequestContext::cancelVoteRequestTimer);
        getRepository().getAllRequestContexts().forEach(ServerRequestContext::cancelFinalDecisionTimer);

        if (getParameters().serverRecoveryTimeS >= 0) {
            getContext().system().scheduler().scheduleOnce(
                    Duration.ofSeconds(getParameters().serverRecoveryTimeS), // delay
                    getSelf(), // receiver
                    new ResumeMessage(), // message
                    getContext().dispatcher(), // executor
                    ActorRef.noSender()); // sender
        }
    }

    @Override
    protected void resume() {
        super.resume();

        getRepository().getAllRequestContexts().forEach(ctx -> {
            switch (ctx.loggedState()) {
                case INIT: {
                    // If the server has not already cast the vote for the transaction, it aborts it.
                    logger.info("Aborting transaction " + ctx.uuid);

                    ctx.log(ServerRequestContext.LogState.GLOBAL_ABORT);
                    ctx.abort();
                    ctx.setProtocolState(TwoPhaseCommitFSM.ABORT);

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
