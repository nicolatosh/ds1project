package it.unitn.arpino.ds1project.nodes.server;

import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.datastore.controller.IDatabaseController;
import it.unitn.arpino.ds1project.datastore.database.DatabaseBuilder;
import it.unitn.arpino.ds1project.messages.ServerInfo;
import it.unitn.arpino.ds1project.messages.TimeoutExpired;
import it.unitn.arpino.ds1project.messages.TimeoutExpired.TIMEOUT_TYPE;
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
    private final IDatabaseController controller;

    /**
     * The other servers in the Data Store that this server can contact in a Two-Phase Commit (2PC) recovery
     */
    private final List<ServerInfo> servers;

    public Server(int lowerKey, int upperKey) {
        controller = DatabaseBuilder.newBuilder()
                .keyRange(lowerKey, upperKey)
                .create();
        servers = new ArrayList<>();
    }

    public static Props props(int lowerKey, int upperKey) {
        return Props.create(Server.class, () -> new Server(lowerKey, upperKey));
    }

    public Receive createReceive() {
        return new ReceiveBuilder()
                .match(ServerInfo.class, this::onServerInfo)
                .match(ReadRequest.class, this::onReadRequest)
                .match(WriteRequest.class, this::onWriteRequest)
                .match(VoteRequest.class, this::onVoteRequest)
                .match(FinalDecision.class, this::onFinalDecision)
                .match(TimeoutExpired.class, this::onTimeoutExpired)
                .match(DecisionResponse.class, this::onDecisionResponse)
                .match(DecisionRequest.class, this::onDecisionRequest)
                .build();
    }

    private void onServerInfo(ServerInfo server) {
        servers.add(server);
    }

    public ServerRequestContext newContext(UUID uuid) {
        ServerRequestContext ctx = new ServerRequestContext(uuid, controller.beginTransaction());
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

        ctx.get().cancelTimer(TIMEOUT_TYPE.VOTE_REQUEST_MISSING);

        ctx.get().prepare();

        switch (ctx.get().getProtocolState()) {
            case READY: {
                logger.info("VOTE_COMMIT");

                VoteResponse vote = new VoteResponse(req.uuid, VoteResponse.Vote.YES);
                getSender().tell(vote, getSelf());
                break;
            }
            case ABORT: {
                VoteResponse vote = new VoteResponse(req.uuid, VoteResponse.Vote.NO);
                getSender().tell(vote, getSelf());
                break;
            }
        }

        ctx.get().startFinalDecisionTimeout(this);
    }

    private void onTimeoutExpired(TimeoutExpired timeout) {
        Optional<ServerRequestContext> ctx = getRequestContext(timeout);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        switch (timeout.getTimeout_type()) {
            case VOTE_REQUEST_MISSING: {
                assert ctx.get().getProtocolState() == ServerRequestContext.TwoPhaseCommitFSM.INIT;
                logger.info("Timeout expired. Reason: did not receive VoteRequest from Coordinator in time");
                logger.info("GLOBAL_ABORT");
                ctx.get().abort();
                break;
            }

            case FINALDECISION_RESPONSE_MISSING: {
                assert ctx.get().getProtocolState() == ServerRequestContext.TwoPhaseCommitFSM.READY;
                logger.info("Timeout expired. Reason: did not receive FinalDecision from Coordinator in time. " +
                        "Starting the Termination Protocol.");
                terminationProtocol(ctx.get());
                break;
            }
        }
    }

    /**
     * The server replies with transaction's outcome (i.e., the coordinator's final decision) to
     * the server requesting it, which is executing the termination protocol.
     */
    private void onDecisionRequest(DecisionRequest req) {
        Optional<ServerRequestContext> ctx = getRequestContext(req);
        if (ctx.isEmpty()) {

            // This case can happen because Servers perform termination protocol by multicast
            // This server might not be part of the same transaction so will send nothing
            return;
        }

        DecisionResponse response;
        UUID uuid = ctx.get().uuid;
        TwoPhaseCommitFSM status = ctx.get().getProtocolState();

        // Other Server is contacting this one because is part of the same transaction.
        // INIT means that this server INIT-timeout did not trigger yet but the transaction ended.
        // At this point this server will stop that timer, ABORT and send INIT to other server.
        if (status.equals(TwoPhaseCommitFSM.INIT)) {
            ctx.get().abort();
        }

        response = new DecisionResponse(uuid, status);
        getSender().tell(response, getSelf());
    }

    private void onDecisionResponse(DecisionResponse resp) {
        Optional<ServerRequestContext> ctx = getRequestContext(resp);
        if (ctx.isEmpty()) {
            return;
        }

        switch (resp.getStatus()) {
            case INIT:
            case ABORT:
                logger.info("Received DecisionResponse, going to ABORT");
                ctx.get().abort();
                break;
            case COMMIT:
                logger.info("Received DecisionResponse, going to COMMIT");
                ctx.get().commit();
                break;
            case READY:
                logger.info("Received DecisionResponse, other server did not know decision");
                break;
        }
    }

    /**
     * Either the {@link Coordinator} or another participant is sending the {@link FinalDecision} to this Server.
     */
    private void onFinalDecision(FinalDecision req) {
        Optional<ServerRequestContext> ctx = getRequestContext(req);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        // We consider only case in which this server is in READY, so it does not know the transaction outcome.
        // If not READY, below situation can happen:
        // We are receiving the FinalDecision from the Coordinator, which has just woken up after a crash, but
        // we already know the FinalDecision as another participant has already sent it to us
        if (ctx.get().getProtocolState().equals(TwoPhaseCommitFSM.READY)) {
            ctx.get().cancelTimer(TIMEOUT_TYPE.FINALDECISION_RESPONSE_MISSING);

            switch (req.decision) {
                case GLOBAL_COMMIT: {
                    ctx.get().commit();
                    break;
                }
                case GLOBAL_ABORT: {
                    ctx.get().abort();
                    break;
                }
            }
        }

        /*
        switch (ctx.get().getProtocolState()) {

            // is it possible? NO. Coordinator do not send Final decision does not receive all votes!
            case INIT: {
                ctx.get().cancelTimer();
                assert req.decision == FinalDecision.Decision.GLOBAL_ABORT;
                ctx.get().abort();
                break;
            }
            case READY: {
                ctx.get().cancelTimer();
                switch (req.decision) {
                    case GLOBAL_COMMIT: {
                        ctx.get().commit();
                        break;
                    }
                    case GLOBAL_ABORT: {
                        ctx.get().abort();
                        break;
                    }
                }
                break;
            }
            default: {
                // (1) we are receiving the FinalDecision from the Coordinator, which has just woken up after a crash, but
                // we already know the FinalDecision as another participant has already sent it to us,
                // (2) we are receiving the FinalDecision from a participant, but we already know it aas another participant
                // has already sent it to us.
                break;
            }
        }
        */
    }

    /**
     * The server asks every other server for the {@link FinalDecision} with a {@link DecisionRequest} and remains
     * blocked until it receives a response.
     *
     * @param ctx Context for which to start the termination protocol
     */
    private void terminationProtocol(ServerRequestContext ctx) {
        DecisionRequest request = new DecisionRequest(ctx.uuid);
        servers.stream()
                .map(serverInfo -> serverInfo.server)
                .forEach(server -> server.tell(request, getSelf()));
    }

    @Override
    protected void resume() {
        super.resume();

        List<ServerRequestContext> voteNotCasted = getActive().stream()
                .filter(ctx -> ctx.getProtocolState() == ServerRequestContext.TwoPhaseCommitFSM.INIT)
                .collect(Collectors.toList());

        List<ServerRequestContext> voteCasted = getActive().stream()
                .filter(ctx -> ctx.getProtocolState() == ServerRequestContext.TwoPhaseCommitFSM.READY)
                .collect(Collectors.toList());

        // If the server has not already cast the vote for the transaction, it aborts it.
        voteNotCasted.forEach(ServerRequestContext::abort);

        // If the server has already cast the vote for the transaction, it asks the others about the coordinator's
        // FinalDecision.
        voteCasted.forEach(this::terminationProtocol);
    }
}
