package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.ReceiveBuilder;
import it.unitn.arpino.ds1project.communication.Multicast;
import it.unitn.arpino.ds1project.messages.Transactional;
import it.unitn.arpino.ds1project.messages.Typed;
import it.unitn.arpino.ds1project.messages.client.TxnAcceptMsg;
import it.unitn.arpino.ds1project.messages.client.TxnResultMsg;
import it.unitn.arpino.ds1project.messages.coordinator.*;
import it.unitn.arpino.ds1project.messages.server.*;
import it.unitn.arpino.ds1project.nodes.STATUS;
import it.unitn.arpino.ds1project.nodes.context.ContextManager;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.time.Duration;
import java.util.Optional;

public class Coordinator extends AbstractActor {
    LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    private STATUS status;

    private final Dispatcher dispatcher;

    ContextManager<CoordinatorRequestContext> contextManager;

    public Coordinator() {
        status = STATUS.ALIVE;
        dispatcher = new Dispatcher();
        contextManager = new ContextManager<>();
    }

    public static Props props() {
        return Props.create(Coordinator.class, Coordinator::new);
    }

    @Override
    public void aroundReceive(PartialFunction<Object, BoxedUnit> receive, Object msg) {
        if (msg instanceof Typed) {
            Typed typed = (Typed) msg;

            log.info(getSelf().path().name() +
                    ": received " + typed.getClass().getSimpleName() +
                    " from " + getSender().path().name() +
                    " (" + typed.getType() + ")");
        }

        super.aroundReceive(receive, msg);
    }

    @Override
    public Receive createReceive() {
        return new ReceiveBuilder()
                .match(CoordinatorStartMsg.class, this::onCoordinatorStartMsg)
                .match(TxnBeginMsg.class, this::onTxnBeginMsg)
                .match(TxnEndMsg.class, this::onTxnEndMsg)
                .match(ReadMsg.class, this::onReadMsg)
                .match(WriteMsg.class, this::onWriteMsg)
                .match(VoteResponse.class, this::onVoteResponse)
                .build();
    }

    private Optional<CoordinatorRequestContext> getRequestContext(Transactional msg) {
        return contextManager.contextOf(msg);
    }

    private CoordinatorRequestContext newContext() {
        CoordinatorRequestContext ctx = new CoordinatorRequestContext(getSender());

        contextManager.save(ctx);
        return ctx;
    }

    private void onCoordinatorStartMsg(CoordinatorStartMsg msg) {
        msg.serverKeys.forEach((server, keys) -> keys.forEach(key -> dispatcher.map(server, key)));
    }

    private void onTxnBeginMsg(TxnBeginMsg msg) {
        CoordinatorRequestContext ctx = this.newContext();

        TxnAcceptMsg response = new TxnAcceptMsg(ctx.uuid());
        getSender().tell(response, getSelf());
    }

    /**
     * Effectively starts the Two-phase commit (2PC) protocol
     */
    private void onTxnEndMsg(TxnEndMsg msg) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(msg);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        Multicast multicast = new Multicast()
                .setSender(getSelf())
                // Fixme
                // .addReceivers(ctx.get().contacted)
                .shuffle();

        if (msg.commit) {
            VoteRequest req = new VoteRequest(msg.uuid());
            multicast.setMessage(req);
        } else {
            AbortRequest req = new AbortRequest(msg.uuid());
            multicast.setMessage(req);
        }

        multicast.multicast();

        if (multicast.completed()) {
            ctx.get().state = CoordinatorRequestContext.STATE.WAIT;
        } else {
            status = STATUS.CRASHED;
        }
    }

    private void onVoteResponse(VoteResponse resp) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(resp);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        switch (resp.vote) {
            case YES:
                ctx.get().yesVoters.add(getSender());

                if (ctx.get().yesVoters.size() == ctx.get().participants.size()) {
                    ctx.get().state = CoordinatorRequestContext.STATE.GLOBAL_COMMIT;

                    // multicast GLOBAL_COMMIT to all participants
                    FinalDecision decision = new FinalDecision(resp.uuid(), Decision.GLOBAL_COMMIT);
                    ctx.get().participants.forEach(server -> getContext().system().scheduler().scheduleOnce(
                            Duration.ofSeconds(1), server, decision, getContext().dispatcher(), getSelf()));

                    // tell the client the result of the transaction
                    TxnResultMsg result = new TxnResultMsg(resp.uuid(), true);
                    ctx.get().client.tell(result, getSelf());

                    // the transaction is completed: subsequent requests will begin a new transaction
                    contextManager.remove(ctx.get());
                }
                break;

            case NO:
                // write GLOBAL_ABORT to local log
                ctx.get().state = CoordinatorRequestContext.STATE.GLOBAL_ABORT;


                // multicast GLOBAL_ABORT to all participants
                FinalDecision decision = new FinalDecision(resp.uuid(), Decision.GLOBAL_ABORT);
                ctx.get().participants.forEach(server -> getContext().system().scheduler().scheduleOnce(
                        Duration.ofSeconds(1), server, decision, getContext().dispatcher(), getSelf()));

                // tell the client the result of the transaction
                TxnResultMsg result = new TxnResultMsg(resp.uuid(), false);
                ctx.get().client.tell(result, getSelf());

                // the transaction is completed: subsequent requests will begin a new transaction
                contextManager.remove(ctx.get());

                break;
        }
    }

    private void onReadMsg(ReadMsg msg) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(msg);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        ReadRequest req = new ReadRequest(msg.uuid(), msg.key);

        ActorRef server = dispatcher.byKey(msg.key);
        server.tell(req, getSelf());

        ctx.get().participants.add(server);
    }

    private void onWriteMsg(WriteMsg msg) {
        Optional<CoordinatorRequestContext> ctx = getRequestContext(msg);
        if (ctx.isEmpty()) {
            // Todo: Bad request
            return;
        }

        WriteRequest req = new WriteRequest(msg.uuid(), msg.key, msg.key);

        ActorRef server = dispatcher.byKey(msg.key);
        server.tell(req, getSelf());

        ctx.get().participants.add(server);
    }
}
