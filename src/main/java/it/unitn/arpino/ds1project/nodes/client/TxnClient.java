package it.unitn.arpino.ds1project.nodes.client;


import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.arpino.ds1project.messages.StartMessage;
import it.unitn.arpino.ds1project.messages.TimeoutMsg;
import it.unitn.arpino.ds1project.messages.TxnMessage;
import it.unitn.arpino.ds1project.messages.client.CoordinatorList;
import it.unitn.arpino.ds1project.messages.client.ReadResultMsg;
import it.unitn.arpino.ds1project.messages.client.TxnAcceptMsg;
import it.unitn.arpino.ds1project.messages.client.TxnResultMsg;
import it.unitn.arpino.ds1project.messages.coordinator.ReadMsg;
import it.unitn.arpino.ds1project.messages.coordinator.TxnBeginMsg;
import it.unitn.arpino.ds1project.messages.coordinator.TxnEndMsg;
import it.unitn.arpino.ds1project.messages.coordinator.WriteMsg;
import it.unitn.arpino.ds1project.simulation.ClientParameters;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class TxnClient extends AbstractActor {
    private final ClientParameters parameters;

    private static final double COMMIT_PROBABILITY = 0.8;
    private static final double WRITE_PROBABILITY = 0.5;
    private static final long MIN_BACKOFF_S = 2;
    private static final long MAX_BACKOFF_S = 8;

    private final Logger logger;

    private final Map<UUID, ClientRequestContext> contexts;

    private final List<ActorRef> coordinators;

    // the maximum key associated to items of the store
    private int maxKey;

    // keep track of the number of TXNs (attempted, successfully committed)
    private int numAttemptedTxn;
    private int numCommittedTxn;

    private int numTransactionsLeft;

    public TxnClient() {
        parameters = new ClientParameters();

        try (InputStream config = TxnClient.class.getResourceAsStream("/logging.properties")) {
            if (config != null) {
                LogManager.getLogManager().readConfiguration(config);
            }
        } catch (IOException ignored) {
        }

        logger = Logger.getLogger(getSelf().path().name());
        contexts = new HashMap<>();
        coordinators = new ArrayList<>();

        numTransactionsLeft = parameters.numTransactions;
    }

    public static Props props() {
        return Props.create(TxnClient.class, TxnClient::new).withDispatcher("my-pinned-dispatcher");
    }

    public ClientParameters getParameters() {
        return parameters;
    }

    public int getNumTransactionsLeft() {
        return numTransactionsLeft;
    }

    private Duration randomBackoff() {
        long seconds = ThreadLocalRandom.current().nextLong(MIN_BACKOFF_S, MAX_BACKOFF_S);
        return Duration.ofSeconds(seconds);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CoordinatorList.class, this::onCoordinatorList)
                .match(StartMessage.class, this::onStartMsg)
                .match(TxnAcceptMsg.class, this::onTxnAcceptMsg)
                .match(ReadResultMsg.class, this::onReadResultMsg)
                .match(TxnResultMsg.class, this::onTxnResultMsg)
                .match(TimeoutMsg.class, this::onTimeoutMsg)
                .build();
    }

    @Override
    public void aroundReceive(PartialFunction<Object, BoxedUnit> receive, Object obj) {
        if (obj instanceof TxnMessage) {
            TxnMessage msg = (TxnMessage) obj;

            logger.info("Received " + msg + " from " + getSender().path().name());

            var ctx = contexts.get(msg.uuid);
            if (ctx.isDecided()) {
                if (!(msg instanceof TxnResultMsg)) {
                    logger.info("The decision is already known (" + ctx.getStatus() + ")");
                    return;
                }
                // else: let the TxnResultMsg pass anyway
            }

        }

        super.aroundReceive(receive, obj);
    }

    private void onCoordinatorList(CoordinatorList msg) {
        coordinators.addAll(msg.coordinators);
        if (coordinators.isEmpty()) {
            logger.info("No available coordinators");
        } else {
            logger.info("Available coordinators: " + coordinators.stream()
                    .map(coordinator -> coordinator.path().name())
                    .collect(Collectors.joining(", ")));

            this.maxKey = msg.maxKey;
        }
    }

    /**
     * Starts a new transaction. Will time out if the coordinator does not reply in time.
     */
    void onStartMsg(StartMessage msg) {
        if (coordinators.isEmpty()) {
            logger.info("No available coordinators. Nothing to do.");
            return;
        }

        // choose a random coordinator to contact
        var coordinator = coordinators.get(ThreadLocalRandom.current().nextInt(coordinators.size()));
        // choose a random number of read operations to perform
        var numOp = ThreadLocalRandom.current().nextInt(parameters.clientMinTxnLength, parameters.clientMaxTxnLength + 1);

        var ctx = new ClientRequestContext(coordinator, numOp);
        contexts.put(ctx.uuid, ctx);

        var begin = new TxnBeginMsg(ctx.uuid);
        coordinator.tell(begin, getSelf());

        ctx.setStatus(ClientRequestContext.Status.REQUESTED);

        ctx.startTimer(this, ClientRequestContext.TXN_ACCEPT_TIMEOUT_S);
        ++numAttemptedTxn;
    }

    private void onTxnAcceptMsg(TxnAcceptMsg msg) {
        var ctx = contexts.get(msg.uuid);
        ctx.setStatus(ClientRequestContext.Status.CONVERSATIONAL);

        ctx.cancelTimer();
        readTwo(ctx);
    }

    private void onReadResultMsg(ReadResultMsg msg) {
        var ctx = contexts.get(msg.uuid);

        ctx.cancelTimer();

        var op = ctx.getOp();
        if (msg.key == op.firstKey) {
            op.firstValue = msg.value;
        } else if (msg.key == op.secondKey) {
            op.secondValue = msg.value;
        } else {
            throw new IllegalStateException("Unexpected key (expected: " + op.firstKey + " or " + op.secondKey + ", found " + msg.key);
        }

        if (!op.isDone()) {
            // give the coordinator more time
            ctx.startTimer(this, ClientRequestContext.READ_TIMEOUT_S);
            return;
        }

        if (ThreadLocalRandom.current().nextDouble() < WRITE_PROBABILITY) {
            writeTwo(ctx);
            return;
        }

        if (ctx.opLeft() > 0) {
            readTwo(ctx);
            return;
        }

        endTxn(ctx);
    }

    void readTwo(ClientRequestContext ctx) {
        int randKeyOffset = 1 + ThreadLocalRandom.current().nextInt(maxKey - 1);
        int key1 = ThreadLocalRandom.current().nextInt(maxKey + 1);
        int key2 = (key1 + randKeyOffset) % (maxKey + 1);

        var op = ctx.newOp(key1, key2);

        var read1 = new ReadMsg(ctx.uuid, op.firstKey);
        var read2 = new ReadMsg(ctx.uuid, op.secondKey);

        ctx.subject.tell(read1, getSelf());
        try {
            Thread.sleep(25);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        ctx.subject.tell(read2, getSelf());

        ctx.startTimer(this, ClientRequestContext.READ_TIMEOUT_S);
    }

    void writeTwo(ClientRequestContext ctx) {
        var op = ctx.getOp();
        if (op.firstValue == null || op.secondValue == null) {
            throw new IllegalStateException("writeTwo was called, but op variables are null");
        }
        if (op.firstValue < 0 || op.secondValue < 0) {
            throw new IllegalStateException("writeTwo was called, but op variables are < 0");
        }

        var amountTaken = 0;
        if (op.firstValue >= 1) {
            amountTaken = 1 + ThreadLocalRandom.current().nextInt(op.firstValue);
        }

        var write1 = new WriteMsg(ctx.uuid, op.firstKey, op.firstValue - amountTaken);
        try {
            Thread.sleep(25);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        var write2 = new WriteMsg(ctx.uuid, op.secondKey, op.secondValue + amountTaken);

        ctx.subject.tell(write1, getSelf());
        ctx.subject.tell(write2, getSelf());

        if (ctx.opLeft() > 0) {
            readTwo(ctx);
            return;
        }

        endTxn(ctx);
    }

    void endTxn(ClientRequestContext ctx) {
        if (ThreadLocalRandom.current().nextDouble() < COMMIT_PROBABILITY) {
            ctx.setStatus(ClientRequestContext.Status.PENDING);

            var end = new TxnEndMsg(ctx.uuid, true);
            ctx.subject.tell(end, getSelf());

            ctx.startTimer(this, ClientRequestContext.TXN_RESULT_TIMEOUT_S);

        } else {
            ctx.setStatus(ClientRequestContext.Status.ABORT);
            --numAttemptedTxn;

            var end = new TxnEndMsg(ctx.uuid, false);
            ctx.subject.tell(end, getSelf());

            // useless to start the timer: even if the coordinator misses this message, it will time out and abort

            if (parameters.clientLoop || numTransactionsLeft > 0) {
                // start a new transaction
                var start = new StartMessage();
                getContext().getSystem().getScheduler().scheduleOnce(
                        randomBackoff(), // delay
                        getSelf(), // receiver
                        start, // message
                        getContext().dispatcher(), // executor
                        getSelf()); // sender
            }
        }
    }

    private void onTxnResultMsg(TxnResultMsg msg) {
        var ctx = contexts.get(msg.uuid);

        // aroundReceive lets old TxnResultMsg pass through, so that we can do a consistency check here
        switch (ctx.getStatus()) {
            case CREATED:
            case REQUESTED: {
                logger.severe("Invalid state: " + ctx.getStatus());
                return;
            }
            case CONVERSATIONAL: {
                if (msg.commit) {
                    // sanity check
                    logger.severe("Received COMMIT, but state is CONVERSATIONAL");
                    return;
                }
                break;
            }
            case PENDING: {
                ctx.cancelTimer();

                if (msg.commit) {
                    ctx.setStatus(ClientRequestContext.Status.COMMIT);
                    ++numCommittedTxn;
                    logger.info("COMMIT OK (" + numCommittedTxn + "/" + numAttemptedTxn + ")");
                } else {
                    ctx.setStatus(ClientRequestContext.Status.ABORT);
                    logger.info("COMMIT FAIL (" + (numAttemptedTxn - numCommittedTxn) + "/" + numAttemptedTxn + ")");
                }

                --numTransactionsLeft;

                if (parameters.clientLoop || numTransactionsLeft > 0) {
                    // start a new transaction
                    var start = new StartMessage();
                    getContext().getSystem().getScheduler().scheduleOnce(
                            randomBackoff(), // delay
                            getSelf(), // receiver
                            start, // message
                            getContext().dispatcher(), // executor
                            getSelf()); // sender
                }
                break;
            }
            case COMMIT: {
                if (!msg.commit) {
                    // sanity check
                    logger.severe("Received ABORT, but state is COMMIT");
                    return;
                }
                break;
            }
            case ABORT: {
                if (msg.commit) {
                    // sanity check
                    logger.severe("Received COMMIT, but state is ABORT");
                    return;
                }
                break;
            }
        }
    }

    private void onTimeoutMsg(TimeoutMsg timeout) {
        var ctx = contexts.get(timeout.uuid);

        switch (ctx.getStatus()) {
            case CREATED: {
                logger.severe("Invalid state (CREATED)");
                break;
            }
            case REQUESTED: {
                // If we send a TxnEndMsg, and the coordinator did not previously receive our TxnBeginMsg,
                // it will print SEVERE. So, we let the coordinator time out.
                logger.info("State is " + ctx.getStatus() + ": aborting, backing off and retrying");
                ctx.setStatus(ClientRequestContext.Status.ABORT);

                --numTransactionsLeft;

                if (parameters.clientLoop || numTransactionsLeft > 0) {
                    // start a new transaction
                    var start = new StartMessage();
                    getContext().getSystem().getScheduler().scheduleOnce(
                            randomBackoff(), // delay
                            getSelf(), // receiver
                            start, // message
                            getContext().dispatcher(), // executor
                            getSelf()); // sender
                }

                break;
            }
            // timeout while waiting for TxnAcceptMsg
            case CONVERSATIONAL: {
                // timeout while waiting for a read response
                logger.info("State is " + ctx.getStatus() + ": aborting, backing off and retrying");
                ctx.setStatus(ClientRequestContext.Status.ABORT);

                --numTransactionsLeft;

                var end = new TxnEndMsg(ctx.uuid, false);
                ctx.subject.tell(end, getSelf());

                if (parameters.clientLoop || numTransactionsLeft > 0) {
                    // start a new transaction
                    var start = new StartMessage();
                    getContext().getSystem().getScheduler().scheduleOnce(
                            randomBackoff(), // delay
                            getSelf(), // receiver
                            start, // message
                            getContext().dispatcher(), // executor
                            getSelf()); // sender
                }

                break;
            }
            case PENDING: {
                // this state means that the client requested to commit.
                // the client should poll for the result.
                logger.info("State is PENDING: polling the coordinator");

                var end = new TxnEndMsg(ctx.uuid, false);
                ctx.subject.tell(end, getSelf());

                ctx.startTimer(this, ClientRequestContext.TXN_RESULT_TIMEOUT_S);
                break;
            }
            // COMMIT and ABORT are already captured by aroundReceive, which checks isDecided
        }
    }
}