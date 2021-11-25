package it.unitn.arpino.ds1project.nodes.server;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.datastore.controller.IDatabaseController;
import it.unitn.arpino.ds1project.datastore.database.DatabaseBuilder;
import it.unitn.arpino.ds1project.datastore.database.IDatabase;
import it.unitn.arpino.ds1project.messages.JoinMessage;
import it.unitn.arpino.ds1project.messages.ResumeMessage;
import it.unitn.arpino.ds1project.messages.coordinator.ReadResult;
import it.unitn.arpino.ds1project.messages.coordinator.VoteResponse;
import it.unitn.arpino.ds1project.messages.server.*;
import it.unitn.arpino.ds1project.nodes.DataStoreNode;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.ServerRequestContext.TwoPhaseCommitFSM;
import it.unitn.arpino.ds1project.simulation.Communication;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
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

    public void addServer(ActorRef server) {
        servers.add(server);
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
                .build();
    }

    @Override
    protected void onJoinMessage(JoinMessage msg) {
        logger.info(getSender().path().name() + " joined");
        servers.add(getSender());
    }

    /**
     * Creates a new context and adds it to the list of contexts so that it can be later retrieved.
     *
     * @param uuid Identifier to assign to the new context.
     * @return The newly created context.
     */
    public ServerRequestContext createNewContext(UUID uuid) {
        ServerRequestContext ctx = new ServerRequestContext(uuid, controller.beginTransaction());
        addContext(ctx);
        return ctx;
    }


    private void onReadRequest(ReadRequest req) {
        Optional<ServerRequestContext> ctx = getRequestContext(req.uuid);
        if (ctx.isEmpty()) {
            ctx = Optional.of(createNewContext(req.uuid));

            ctx.get().log(ServerRequestContext.LogState.INIT);
            ctx.get().setProtocolState(TwoPhaseCommitFSM.INIT);
            ctx.get().startVoteRequestTimer(this);
        }

        if (ctx.get().loggedState() != ServerRequestContext.LogState.INIT) {
            logger.severe("Invalid protocol state (" + ctx.get().loggedState() + ", should be INIT)");
            return;
        }

        int value = ctx.get().read(req.key);

        ReadResult res = new ReadResult(req.uuid, req.key, value);
        getSender().tell(res, getSelf());
    }

    private void onWriteRequest(WriteRequest req) {
        Optional<ServerRequestContext> ctx = getRequestContext(req.uuid);
        if (ctx.isEmpty()) {
            ctx = Optional.of(createNewContext(req.uuid));

            ctx.get().log(ServerRequestContext.LogState.INIT);
            ctx.get().setProtocolState(TwoPhaseCommitFSM.INIT);
            ctx.get().startVoteRequestTimer(this);
        }

        if (ctx.get().loggedState() != ServerRequestContext.LogState.INIT) {
            logger.severe("Invalid protocol state (" + ctx.get().loggedState() + ", should be INIT)");
            return;
        }

        ctx.get().write(req.key, req.value);
    }

    private void onVoteRequest(VoteRequest req) {
        Optional<ServerRequestContext> ctx = getRequestContext(req.uuid);
        if (ctx.isEmpty()) {
            logger.severe("Bad request");
            return;
        }

        if (ctx.get().loggedState() != ServerRequestContext.LogState.INIT) {
            logger.severe("Invalid logged state (" + ctx.get().loggedState() + ", should be INIT)");
            return;
        }

        ctx.get().cancelVoteRequestTimer();

        if (ctx.get().prepare()) {
            ctx.get().log(ServerRequestContext.LogState.VOTE_COMMIT);

            VoteResponse vote = new VoteResponse(req.uuid, VoteResponse.Vote.YES);
            if (!Communication.unicast(getSelf(), getSender(), vote, getParameters().serverOnVoteResponseCrashProbability)) {
                crash();
                return;
            }

            ctx.get().setProtocolState(TwoPhaseCommitFSM.READY);

            ctx.get().startFinalDecisionTimer(this);
        } else {
            ctx.get().log(ServerRequestContext.LogState.GLOBAL_ABORT);

            VoteResponse vote = new VoteResponse(req.uuid, VoteResponse.Vote.NO);
            if (!Communication.unicast(getSelf(), getSender(), vote, getParameters().serverOnVoteResponseCrashProbability)) {
                crash();
                return;
            }

            ctx.get().setProtocolState(TwoPhaseCommitFSM.ABORT);

            // if we could not prepare the transaction and have aborted, we do not have to wait for a final decision,
            // thus we must not start the final decision timer.
        }
    }

    private void onVoteRequestTimeout(VoteRequestTimeout timeout) {
        Optional<ServerRequestContext> ctx = getRequestContext(timeout.uuid);
        if (ctx.isEmpty()) {
            logger.severe("Bad request");
            return;
        }

        if (ctx.get().loggedState() != ServerRequestContext.LogState.INIT) {
            logger.severe("Invalid logged state (" + ctx.get().loggedState() + ", should be INIT)");
            return;
        }

        logger.info("Aborting the transaction");

        ctx.get().log(ServerRequestContext.LogState.GLOBAL_ABORT);
        ctx.get().abort();
        ctx.get().setProtocolState(TwoPhaseCommitFSM.ABORT);
    }

    private void onFinalDecisionTimeout(FinalDecisionTimeout timeout) {
        Optional<ServerRequestContext> ctx = getRequestContext(timeout.uuid);
        if (ctx.isEmpty()) {
            logger.severe("Bad request");
            return;
        }

        if (ctx.get().loggedState() != ServerRequestContext.LogState.VOTE_COMMIT) {
            logger.severe("Invalid logged state (" + ctx.get().loggedState() + ", should be VOTE_COMMIT)");
            return;
        }

        logger.info("Starting the termination protocol");
        terminationProtocol(ctx.get());
    }

    /**
     * A server which is executing the termination protocol is requesting to this server the final decision of a transaction.
     * If this server is not participating in that transaction, it ignores the request.
     */
    private void onDecisionRequest(DecisionRequest req) {
        Optional<ServerRequestContext> ctx = getRequestContext(req.uuid);
        if (ctx.isEmpty()) {
            // Ignore the request
            return;
        }

        DecisionResponse response = null;

        switch (ctx.get().loggedState()) {
            case INIT: {
                logger.severe("Invalid logged state (INIT, should be VOTE_COMMIT, DECISION or GLOBAL_ABORT)");
                return;
            }
            case VOTE_COMMIT: {
                response = new DecisionResponse(ctx.get().uuid, DecisionResponse.Decision.UNKNOWN);
                break;
            }
            case DECISION: {
                switch (ctx.get().getProtocolState()) {
                    case ABORT: {
                        response = new DecisionResponse(ctx.get().uuid, DecisionResponse.Decision.GLOBAL_ABORT);
                        break;
                    }
                    case COMMIT: {
                        response = new DecisionResponse(ctx.get().uuid, DecisionResponse.Decision.GLOBAL_COMMIT);
                        break;
                    }
                }
                break;
            }
            case GLOBAL_ABORT: {
                response = new DecisionResponse(ctx.get().uuid, DecisionResponse.Decision.GLOBAL_ABORT);
                break;
            }
        }

        if (!Communication.unicast(getSelf(), getSender(), response, getParameters().serverOnVoteResponseCrashProbability)) {
            crash();
        }
    }

    private void onDecisionResponse(DecisionResponse resp) {
        Optional<ServerRequestContext> ctx = getRequestContext(resp.uuid);
        if (ctx.isEmpty()) {
            logger.severe("Bad request");
            return;
        }

        switch (ctx.get().loggedState()) {
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
                        ctx.get().log(ServerRequestContext.LogState.DECISION);
                        ctx.get().commit();
                        ctx.get().setProtocolState(TwoPhaseCommitFSM.COMMIT);
                        break;
                    }
                    case GLOBAL_ABORT: {
                        logger.info("Received GLOBAL_ABORT: aborting");
                        ctx.get().log(ServerRequestContext.LogState.DECISION);
                        ctx.get().abort();
                        ctx.get().setProtocolState(TwoPhaseCommitFSM.ABORT);
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
        Optional<ServerRequestContext> ctx = getRequestContext(req.uuid);
        if (ctx.isEmpty()) {
            logger.info("Context is empty: a read or write request was received while the server was crashed");
            return;
        }

        switch (ctx.get().loggedState()) {
            case INIT: {
                logger.info("Received while in INIT: client abort");

                ctx.get().cancelVoteRequestTimer();

                ctx.get().log(ServerRequestContext.LogState.DECISION);
                ctx.get().abort();
                ctx.get().setProtocolState(TwoPhaseCommitFSM.ABORT);

                break;
            }
            case VOTE_COMMIT: {
                // This line used to cause a null pointer exception:
                // There is a case in which the final decision timer was never started.
                // It happens when, in the function onVoteRequest, in the true branch of branch ctx.get().prepare(),
                // the server crashes and thus does not reach the line with ctx.get().startFinalDecisionTimer(this).
                ctx.get().cancelFinalDecisionTimer();

                switch (req.decision) {
                    case GLOBAL_COMMIT: {
                        logger.info("Received GLOBAL_COMMIT: committing");
                        ctx.get().log(ServerRequestContext.LogState.DECISION);
                        ctx.get().commit();
                        ctx.get().setProtocolState(TwoPhaseCommitFSM.COMMIT);
                        break;
                    }
                    case GLOBAL_ABORT: {
                        logger.info("Received GLOBAL_ABORT: aborting");
                        ctx.get().log(ServerRequestContext.LogState.DECISION);
                        ctx.get().abort();
                        ctx.get().setProtocolState(TwoPhaseCommitFSM.ABORT);
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
    }

    /**
     * The server asks every other server for the {@link FinalDecision} with a {@link DecisionRequest} and remains
     * blocked until it receives a response.
     *
     * @param ctx Context for which to start the termination protocol
     */
    private void terminationProtocol(ServerRequestContext ctx) {
        DecisionRequest request = new DecisionRequest(ctx.uuid);
        if (!Communication.multicast(getSelf(), servers, request, getParameters().serverOnDecisionRequestCrashProbability)) {
            crash();
        }
    }

    @Override
    protected void crash() {
        super.crash();

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

        List<ServerRequestContext> voteNotCasted = getActive().stream()
                .filter(ctx -> ctx.loggedState() == ServerRequestContext.LogState.INIT)
                .collect(Collectors.toList());

        List<ServerRequestContext> voteCasted = getActive().stream()
                .filter(ctx -> ctx.loggedState() == ServerRequestContext.LogState.VOTE_COMMIT)
                .collect(Collectors.toList());

        // If the server has not already cast the vote for the transaction, it aborts it.
        voteNotCasted.forEach(ctx -> {
            ctx.log(ServerRequestContext.LogState.GLOBAL_ABORT);
            ctx.abort();
            ctx.setProtocolState(TwoPhaseCommitFSM.ABORT);
        });

        // If the server has already cast the vote for the transaction, it asks the others about the coordinator's
        // FinalDecision.
        voteCasted.forEach(this::terminationProtocol);
    }
}
