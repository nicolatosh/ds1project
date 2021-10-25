package it.unitn.arpino.ds1project.nodes.server;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.datastore.DatabaseBuilder;
import it.unitn.arpino.ds1project.datastore.IDatabaseController;
import it.unitn.arpino.ds1project.messages.Transactional;
import it.unitn.arpino.ds1project.messages.coordinator.ReadResult;
import it.unitn.arpino.ds1project.messages.coordinator.VoteResponse;
import it.unitn.arpino.ds1project.messages.server.*;
import it.unitn.arpino.ds1project.nodes.AbstractNode;
import it.unitn.arpino.ds1project.nodes.STATUS;
import it.unitn.arpino.ds1project.nodes.context.ContextManager;

import java.util.List;
import java.util.Optional;

public class Server extends AbstractNode {
    private IDatabaseController controller;

    /**
     * The other servers in the Data Store that this server can contact in a Two-Phase Commit (2PC) recovery
     */
    private List<ActorRef> servers;

    ContextManager<ServerRequestContext> contextManager;

    public Server(int lowerKey, int upperKey) {
        status = STATUS.ALIVE;
        controller = DatabaseBuilder.newBuilder()
                .keyRange(lowerKey, upperKey)
                .create();
        contextManager = new ContextManager<>();
    }

    public static Props props(int lowerKey, int upperKey) {
        return Props.create(Server.class, () -> new Server(lowerKey, upperKey));
    }

    public Receive createReceive() {
        return new ReceiveBuilder()
                .match(ServerStartMsg.class, this::onServerStartMsg)
                .match(ReadRequest.class, this::onReadRequest)
                .match(WriteRequest.class, this::onWriteRequest)
                .match(VoteRequest.class, this::onVoteRequest)
                .match(FinalDecision.class, this::onFinalDecision)
                .match(AbortRequest.class, this::onAbortRequest)
                .build();
    }

    private Optional<ServerRequestContext> getRequestContext(Transactional msg) {
        return contextManager.contextOf(msg);
    }

    private ServerRequestContext newContext(Transactional msg) {
        ServerRequestContext ctx = new ServerRequestContext(msg.uuid(), controller.beginTransaction());

        contextManager.setActive(ctx);
        return ctx;
    }

    private void onServerStartMsg(ServerStartMsg msg) {
        servers = msg.servers;
    }

    private void onReadRequest(ReadRequest req) {
        ServerRequestContext ctx = getRequestContext(req).orElse(this.newContext(req));

        int value = ctx.read(req.key);

        ReadResult res = new ReadResult(req.uuid(), req.key, value);
        getSender().tell(res, getSelf());
    }

    private void onWriteRequest(WriteRequest req) {
        ServerRequestContext ctx = getRequestContext(req).orElse(this.newContext(req));

        ctx.write(req.key, req.value);
    }

    private void onVoteRequest(VoteRequest req) {
        Optional<ServerRequestContext> ctx = getRequestContext(req);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        ctx.get().prepare();

        VoteResponse vote = null;

        switch (ctx.get().getProtocolState()) {
            case VOTE_COMMIT:
            case READY:
                vote = new VoteResponse(req.uuid(), Vote.YES);
                break;
            case GLOBAL_ABORT:
                vote = new VoteResponse(req.uuid(), Vote.NO);
                ctx.get().abort();
                break;
        }

        getSender().tell(vote, getSelf());
    }

    private void onAbortRequest(AbortRequest msg) {
        Optional<ServerRequestContext> ctx = getRequestContext(msg);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        // TODO if we do that "locally" then we need to manage the situation
        // in which coordinator tells us to abort again so the line below
        // will get executed twice (see OnDecisionRequest)
        //ctx.get().abort();

        VoteResponse vote = new VoteResponse(msg.uuid(), Vote.NO);
        getSender().tell(vote, getSelf());

        contextManager.setCompleted(ctx.get());
    }

    private void onFinalDecision(FinalDecision req) {
        Optional<ServerRequestContext> ctx = getRequestContext(req);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        switch (req.decision) {
            case GLOBAL_COMMIT:
                ctx.get().commit();
                break;

            case GLOBAL_ABORT:
                ctx.get().abort();
                break;
        }

        contextManager.setCompleted(ctx.get());
    }
}
