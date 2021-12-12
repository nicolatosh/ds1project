package it.unitn.arpino.ds1project.simulation;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.datastore.database.DatabaseBuilder;
import it.unitn.arpino.ds1project.messages.JoinMessage;
import it.unitn.arpino.ds1project.messages.StartMessage;
import it.unitn.arpino.ds1project.messages.client.ReadResultMsg;
import it.unitn.arpino.ds1project.messages.client.TxnAcceptMsg;
import it.unitn.arpino.ds1project.messages.client.TxnResultMsg;
import it.unitn.arpino.ds1project.messages.coordinator.ReadMsg;
import it.unitn.arpino.ds1project.messages.coordinator.TxnBeginMsg;
import it.unitn.arpino.ds1project.messages.coordinator.TxnEndMsg;
import it.unitn.arpino.ds1project.messages.coordinator.WriteMsg;
import it.unitn.arpino.ds1project.nodes.coordinator.Coordinator;
import it.unitn.arpino.ds1project.nodes.coordinator.CoordinatorRequestContext;
import it.unitn.arpino.ds1project.nodes.server.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.stream.IntStream;

public class EndToEnd {
    Random random;
    final int N_TRANSACTIONS = 5;
    final int BACKOFF_S = 1;
    final int initialSum = 20 * DatabaseBuilder.DEFAULT_DATA_VALUE;


    ActorSystem system;
    TestActorRef<Server> server0, server1;
    TestActorRef<Coordinator> coordinator;

    @BeforeEach
    void setUp() {
        random = new Random();

        system = ActorSystem.create();

        server0 = TestActorRef.create(system, Server.props(0, 9), "server0");
        server1 = TestActorRef.create(system, Server.props(10, 19), "server1");

        coordinator = TestActorRef.create(system, Coordinator.props(), "coordinator");
        coordinator.tell(new JoinMessage(10, 19), server1);
        coordinator.tell(new JoinMessage(0, 9), server0);

        var start = new StartMessage();
        coordinator.tell(start, ActorRef.noSender());

    }

    @AfterEach
    void tearDown() {
        TestKit.shutdownActorSystem(system, Duration.create(1, TimeUnit.SECONDS), true);
        system = null;
        server0 = server1 = null;
        coordinator = null;
    }

    private void checkConsistency() throws InterruptedException {
        while (!coordinator.underlyingActor().getRepository().getAllRequestContexts().stream()
                .allMatch(CoordinatorRequestContext::allParticipantsDone)) {
            Thread.sleep(BACKOFF_S * 1000);
        }

        int currentSum = IntStream.rangeClosed(0, 9).map(key -> server0.underlyingActor().getDatabase().read(key)).sum() +
                IntStream.rangeClosed(10, 19).map(key -> server1.underlyingActor().getDatabase().read(key)).sum();

        Assertions.assertEquals(initialSum, currentSum);
    }

    @Test
    void sequentialTransactionsWithCoordinatorCrashes() throws InterruptedException {
        coordinator.underlyingActor().getParameters().coordinatorOnVoteRequestSuccessProbability = 0.55;
        coordinator.underlyingActor().getParameters().coordinatorOnFinalDecisionSuccessProbability = 0.55;

        int txnLeft = N_TRANSACTIONS;
        while (txnLeft > 0) {

            checkConsistency();

            MyTestKit testKit = new MyTestKit(system);
            testKit.logger.info(txnLeft + " transactions left");

            var uuid = UUID.randomUUID();
            var begin = new TxnBeginMsg(uuid);
            coordinator.tell(begin, testKit.testActor());

            try {
                testKit.expectMsg(new TxnAcceptMsg(uuid));
            } catch (AssertionError err) {
                testKit.logger.info(testKit.testActor().path().name() + ": timeout while waiting for TxnAcceptMsg");
                Thread.sleep(BACKOFF_S * 1000);
                continue;
            }

            int keyFrom = random.nextInt(10);
            int keyTo = 10 + random.nextInt(10);

            var read1 = new ReadMsg(uuid, keyFrom);
            coordinator.tell(read1, testKit.testActor());

            int value1;
            try {
                value1 = testKit.expectMsgClass(ReadResultMsg.class).value;
            } catch (AssertionError err) {
                testKit.logger.info(testKit.testActor().path().name() + ": " + uuid + ": timeout while waiting for ReadResultMsg");
                Thread.sleep(BACKOFF_S * 1000);
                continue;
            }

            var read2 = new ReadMsg(uuid, keyTo);
            coordinator.tell(read2, testKit.testActor());

            int value2;
            try {
                value2 = testKit.expectMsgClass(ReadResultMsg.class).value;
            } catch (AssertionError err) {
                testKit.logger.info(testKit.testActor().path().name() + ": " + uuid + ": timeout while waiting for ReadResultMsg");
                Thread.sleep(BACKOFF_S * 1000);
                continue;
            }

            var write1 = new WriteMsg(uuid, keyFrom, value1 - 1);
            coordinator.tell(write1, testKit.testActor());
            testKit.expectNoMessage();

            var write2 = new WriteMsg(uuid, keyTo, value2 + 1);
            coordinator.tell(write2, testKit.testActor());
            testKit.expectNoMessage();

            var end = new TxnEndMsg(uuid, true);
            coordinator.tell(end, testKit.testActor());

            boolean commit;
            try {
                commit = testKit.expectMsgClass(TxnResultMsg.class).commit;
            } catch (AssertionError err) {
                testKit.logger.info(testKit.testActor().path().name() + ": " + uuid + ": timeout while waiting for TxnResultMsg");
                Thread.sleep(BACKOFF_S * 1000);
                continue;
            }

            testKit.logger.info(testKit.testActor().path().name() + ": Transaction completed! Outcome: " + (commit ? "committed" : "aborted"));

            --txnLeft;
        }

        checkConsistency();
    }

    @Test
    void sequentialTransactionsWithServerCrashes() throws InterruptedException {
        List.of(server0, server1).forEach(server -> {
            server.underlyingActor().getParameters().serverOnVoteResponseSuccessProbability = 0.55;
            server.underlyingActor().getParameters().serverOnDecisionRequestSuccessProbability = 0.55;
            server.underlyingActor().getParameters().serverOnDecisionResponseSuccessProbability = 0.5;
        });

        int txnLeft = N_TRANSACTIONS;
        while (txnLeft > 0) {

            checkConsistency();

            MyTestKit testKit = new MyTestKit(system);
            testKit.logger.info(txnLeft + " transactions left");

            var uuid = UUID.randomUUID();
            var begin = new TxnBeginMsg(uuid);
            coordinator.tell(begin, testKit.testActor());

            try {
                testKit.expectMsg(new TxnAcceptMsg(uuid));
            } catch (AssertionError err) {
                testKit.logger.info(testKit.testActor().path().name() + ": timeout while waiting for TxnAcceptMsg");
                Thread.sleep(BACKOFF_S * 1000);
                continue;
            }

            int keyFrom = random.nextInt(10);
            int keyTo = 10 + random.nextInt(10);

            var read1 = new ReadMsg(uuid, keyFrom);
            coordinator.tell(read1, testKit.testActor());

            int value1;
            try {
                value1 = testKit.expectMsgClass(ReadResultMsg.class).value;
            } catch (AssertionError err) {
                testKit.logger.info(testKit.testActor().path().name() + ": " + uuid + ": timeout while waiting for ReadResultMsg");
                Thread.sleep(BACKOFF_S * 1000);
                continue;
            }

            var read2 = new ReadMsg(uuid, keyTo);
            coordinator.tell(read2, testKit.testActor());

            int value2;
            try {
                value2 = testKit.expectMsgClass(ReadResultMsg.class).value;
            } catch (AssertionError err) {
                testKit.logger.info(testKit.testActor().path().name() + ": " + uuid + ": timeout while waiting for ReadResultMsg");
                Thread.sleep(BACKOFF_S * 1000);
                continue;
            }

            var write1 = new WriteMsg(uuid, keyFrom, value1 - 1);
            coordinator.tell(write1, testKit.testActor());
            testKit.expectNoMessage();

            var write2 = new WriteMsg(uuid, keyTo, value2 + 1);
            coordinator.tell(write2, testKit.testActor());
            testKit.expectNoMessage();

            var end = new TxnEndMsg(uuid, true);
            coordinator.tell(end, testKit.testActor());

            boolean commit;
            try {
                commit = testKit.expectMsgClass(TxnResultMsg.class).commit;
            } catch (AssertionError err) {
                testKit.logger.info(testKit.testActor().path().name() + ": " + uuid + ": timeout while waiting for TxnResultMsg");
                Thread.sleep(BACKOFF_S * 1000);
                continue;
            }

            testKit.logger.info(testKit.testActor().path().name() + ": Transaction completed! Outcome: " + (commit ? "committed" : "aborted"));

            --txnLeft;
        }

        checkConsistency();
    }

    @Test
    void sequentialTransactionsWithCoordinatorAndServerCrashes() throws InterruptedException {
        coordinator.underlyingActor().getParameters().coordinatorOnVoteRequestSuccessProbability = 0.55;
        coordinator.underlyingActor().getParameters().coordinatorOnFinalDecisionSuccessProbability = 0.55;

        List.of(server0, server1).forEach(server -> {
            server.underlyingActor().getParameters().serverOnVoteResponseSuccessProbability = 0.55;
            server.underlyingActor().getParameters().serverOnDecisionRequestSuccessProbability = 0.55;
            server.underlyingActor().getParameters().serverOnDecisionResponseSuccessProbability = 0.55;
        });

        int txnLeft = N_TRANSACTIONS;
        while (txnLeft > 0) {

            checkConsistency();

            MyTestKit testKit = new MyTestKit(system);
            testKit.logger.info(txnLeft + " transactions left");

            var uuid = UUID.randomUUID();
            var begin = new TxnBeginMsg(uuid);
            coordinator.tell(begin, testKit.testActor());

            try {
                testKit.expectMsg(new TxnAcceptMsg(uuid));
            } catch (AssertionError err) {
                testKit.logger.info(testKit.testActor().path().name() + ": timeout while waiting for TxnAcceptMsg");
                Thread.sleep(BACKOFF_S * 1000);
                continue;
            }

            int keyFrom = random.nextInt(10);
            int keyTo = 10 + random.nextInt(10);

            var read1 = new ReadMsg(uuid, keyFrom);
            coordinator.tell(read1, testKit.testActor());

            int value1;
            try {
                value1 = testKit.expectMsgClass(ReadResultMsg.class).value;
            } catch (AssertionError err) {
                testKit.logger.info(testKit.testActor().path().name() + ": " + uuid + ": timeout while waiting for ReadResultMsg");
                Thread.sleep(BACKOFF_S * 1000);
                continue;
            }

            var read2 = new ReadMsg(uuid, keyTo);
            coordinator.tell(read2, testKit.testActor());

            int value2;
            try {
                value2 = testKit.expectMsgClass(ReadResultMsg.class).value;
            } catch (AssertionError err) {
                testKit.logger.info(testKit.testActor().path().name() + ": " + uuid + ": timeout while waiting for ReadResultMsg");
                Thread.sleep(BACKOFF_S * 1000);
                continue;
            }

            var write1 = new WriteMsg(uuid, keyFrom, value1 - 1);
            coordinator.tell(write1, testKit.testActor());
            testKit.expectNoMessage();

            var write2 = new WriteMsg(uuid, keyTo, value2 + 1);
            coordinator.tell(write2, testKit.testActor());
            testKit.expectNoMessage();

            var end = new TxnEndMsg(uuid, true);
            coordinator.tell(end, testKit.testActor());

            boolean commit;
            try {
                commit = testKit.expectMsgClass(TxnResultMsg.class).commit;
            } catch (AssertionError err) {
                testKit.logger.info(testKit.testActor().path().name() + ": " + uuid + ": timeout while waiting for TxnResultMsg");
                Thread.sleep(BACKOFF_S * 1000);
                continue;
            }

            testKit.logger.info(testKit.testActor().path().name() + ": Transaction completed! Outcome: " + (commit ? "committed" : "aborted"));

            --txnLeft;
        }

        checkConsistency();
    }

    private static class MyTestKit extends TestKit {
        protected final Logger logger;

        public MyTestKit(ActorSystem system) {
            super(system);

            try (InputStream config = MyTestKit.class.getResourceAsStream("/logging.properties")) {
                if (config != null) {
                    LogManager.getLogManager().readConfiguration(config);
                }
            } catch (IOException ignored) {
            }

            logger = Logger.getLogger(testActor().path().name());
        }
    }
}

