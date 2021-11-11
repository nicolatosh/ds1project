package it.unitn.arpino.ds1project.nodes.server;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.datastore.controller.IDatabaseController;
import it.unitn.arpino.ds1project.datastore.database.DatabaseBuilder;
import it.unitn.arpino.ds1project.datastore.database.IDatabase;
import it.unitn.arpino.ds1project.messages.coordinator.ReadResult;
import it.unitn.arpino.ds1project.messages.coordinator.VoteResponse;
import it.unitn.arpino.ds1project.messages.server.*;
import it.unitn.arpino.ds1project.nodes.DataStoreNode;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.server.ServerRequestContext.TwoPhaseCommitFSM;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

public class Server extends DataStoreNode<ServerRequestContext> {
    private final IDatabase database;
    private final IDatabaseController controller;

    /**
     * The other servers in the Data Store that this server can contact in a Two-Phase Commit (2PC) recovery
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

    public IDatabase getDatabase() {
        return database;
    }

    public static Props props(int lowerKey, int upperKey) {
        return Props.create(Server.class, () -> new Server(lowerKey, upperKey));
    }

    public Receive createReceive() {
        return new ReceiveBuilder()
                .match(ServerJoin.class, this::onServerJoined)
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

    private void onServerJoined(ServerJoin msg) {
        servers.add(msg.server);
    }

    public ServerRequestContext newContext(UUID uuid) {
        ServerRequestContext ctx = new ServerRequestContext(uuid, controller.beginTransaction());
        ctx.log(ServerRequestContext.LogState.INIT);
        ctx.startVoteRequestTimeout(this);
        addContext(ctx);
        return ctx;
    }


    private void onReadRequest(ReadRequest req) {
        ServerRequestContext ctx = getRequestContext(req).orElse(newContext(req.uuid));

        int value = ctx.read(req.key);

        ReadResult res = new ReadResult(req.uuid, req.key, value);
        getSender().tell(res, getSelf());
    }

    private void onWriteRequest(WriteRequest req) {
        ServerRequestContext ctx = getRequestContext(req).orElse(newContext(req.uuid));

        ctx.write(req.key, req.value);
    }

    private void onVoteRequest(VoteRequest req) {
        Optional<ServerRequestContext> ctx = getRequestContext(req);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        ctx.get().cancelVoteRequestTimeout();

        if (ctx.get().prepare()) {
            ctx.get().log(ServerRequestContext.LogState.VOTE_COMMIT);

            VoteResponse vote = new VoteResponse(req.uuid, VoteResponse.Vote.YES);
            getSender().tell(vote, getSelf());

            ctx.get().setProtocolState(TwoPhaseCommitFSM.READY);
        } else {
            ctx.get().log(ServerRequestContext.LogState.GLOBAL_ABORT);

            VoteResponse vote = new VoteResponse(req.uuid, VoteResponse.Vote.NO);
            getSender().tell(vote, getSelf());

            ctx.get().setProtocolState(TwoPhaseCommitFSM.ABORT);
        }


        ctx.get().startFinalDecisionTimeout(this);
    }

    private void onVoteRequestTimeout(VoteRequestTimeout timeout) {
        Optional<ServerRequestContext> ctx = getRequestContext(timeout);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        logger.info("Timeout expired. Reason: did not receive VoteRequest from Coordinator in time");

        ctx.get().log(ServerRequestContext.LogState.GLOBAL_ABORT);
        ctx.get().abort();
        ctx.get().setProtocolState(TwoPhaseCommitFSM.ABORT);
    }

    private void onFinalDecisionTimeout(FinalDecisionTimeout timeout) {
        Optional<ServerRequestContext> ctx = getRequestContext(timeout);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        assert ctx.get().getProtocolState() == ServerRequestContext.TwoPhaseCommitFSM.READY;

        logger.info("Timeout expired. Reason: did not receive FinalDecision from Coordinator in time. " +
                "Starting the Termination Protocol.");
        terminationProtocol(ctx.get());
    }

    /**
     * The Server replies with transaction's outcome to the server requesting it, which must be executing the
     * termination protocol. If this server is not participating in the transaction for which the decision being
     * requested, it ignores the request.
     */
    private void onDecisionRequest(DecisionRequest req) {
        Optional<ServerRequestContext> ctx = getRequestContext(req);
        if (ctx.isEmpty()) {
            // This server is not participating in the same transaction for which the decision is being requested.
            return;
        }


        // It is possible that this Server is being requested the decision, but it has not yet even received the
        // Coordinator's VoteRequest. Thus, it is safe to abort.
        if (ctx.get().getProtocolState() == TwoPhaseCommitFSM.INIT) {
            ctx.get().abort();
            ctx.get().setProtocolState(TwoPhaseCommitFSM.ABORT);
        }

        DecisionResponse response = new DecisionResponse(ctx.get().uuid, ctx.get().getProtocolState());
        getSender().tell(response, getSelf());
    }

    /**
     * Another participant is sending the final decision to this Server. It is possible that the participant does not
     * know the final decision either, in which case this Server doesn't do anything.
     */
    private void onDecisionResponse(DecisionResponse resp) {
        Optional<ServerRequestContext> ctx = getRequestContext(resp);
        if (ctx.isEmpty()) {
            return;
        }

        switch (resp.getStatus()) {
            case INIT: {
                logger.info("Received INIT. Aborting");
                ctx.get().log(ServerRequestContext.LogState.DECISION);
                ctx.get().abort();
                ctx.get().setProtocolState(TwoPhaseCommitFSM.ABORT);
            }
            case ABORT: {
                logger.info("Received ABORT. Aborting");
                ctx.get().log(ServerRequestContext.LogState.DECISION);
                ctx.get().abort();
                ctx.get().setProtocolState(TwoPhaseCommitFSM.ABORT);
                break;
            }
            case COMMIT: {
                logger.info("Received COMMIT. Committing");
                ctx.get().log(ServerRequestContext.LogState.DECISION);
                ctx.get().commit();
                ctx.get().setProtocolState(TwoPhaseCommitFSM.COMMIT);
                break;
            }
            case READY: {
                logger.info("Received READY. Ignoring");
                break;
            }
        }
    }

    /**
     * The {@link Coordinator} is sending the {@link FinalDecision} to this Server.
     */
    private void onFinalDecision(FinalDecision req) {
        Optional<ServerRequestContext> ctx = getRequestContext(req);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        switch (ctx.get().getProtocolState()) {
            case INIT: {
                if (req.clientAbort) {
                    logger.info("Received while in INIT. Client abort");
                    ctx.get().log(ServerRequestContext.LogState.DECISION);
                    ctx.get().abort();
                    ctx.get().setProtocolState(TwoPhaseCommitFSM.ABORT);
                }
                break;
            }
            case READY: {
                // This is the normal case: the Coordinator has sent the FinalDecision to this Server in time.
                ctx.get().cancelFinalDecisionTimeout();
                switch (req.decision) {
                    case GLOBAL_COMMIT: {
                        logger.info("Received COMMIT. Committing");
                        ctx.get().log(ServerRequestContext.LogState.DECISION);
                        ctx.get().commit();
                        ctx.get().setProtocolState(TwoPhaseCommitFSM.COMMIT);
                        break;
                    }
                    case GLOBAL_ABORT: {
                        logger.info("Received ABORT. Aborting");
                        ctx.get().log(ServerRequestContext.LogState.DECISION);
                        ctx.get().abort();
                        ctx.get().setProtocolState(TwoPhaseCommitFSM.ABORT);
                        break;
                    }
                }
                break;
            }
            case ABORT: {
                // This might be a retransmission from the Coordinator
                logger.info("Received FinalDecision, but I have already completed (ABORT)");
                break;
            }
            case COMMIT: {
                logger.info("Received FinalDecision, but I have already completed (COMMIT)");
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
        servers.forEach(server -> server.tell(request, getSelf()));
    }

    @Override
    protected void resume() {
        super.resume();

        List<ServerRequestContext> voteNotCasted = getActive().stream()
                .filter(ctx -> ctx.loggedState().get() == ServerRequestContext.LogState.INIT)
                .collect(Collectors.toList());

        List<ServerRequestContext> voteCasted = getActive().stream()
                .filter(ctx -> ctx.loggedState().get() == ServerRequestContext.LogState.VOTE_COMMIT)
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
