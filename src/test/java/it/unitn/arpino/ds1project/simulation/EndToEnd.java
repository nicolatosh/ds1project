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
import java.util.concurrent.TimeUnit;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.stream.IntStream;

public class EndToEnd {
    Random random;
    final int N_TRANSACTIONS = 5;
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
        server0.tell(new JoinMessage(10, 19), server1);
        server1.tell(new JoinMessage(0, 9), server0);

        coordinator = TestActorRef.create(system, Coordinator.props(), "coordinator");
        coordinator.tell(new JoinMessage(10, 19), server1);
        coordinator.tell(new JoinMessage(0, 9), server0);

        List.of(server0, server1, coordinator).forEach(node -> node.tell(new StartMessage(), ActorRef.noSender()));

    }

    @AfterEach
    void tearDown() {
        TestKit.shutdownActorSystem(system, Duration.create(1, TimeUnit.SECONDS), true);
        system = null;
        server0 = server1 = null;
        coordinator = null;
    }

    private int calculateSum() {
        return Integer.sum(
                IntStream.rangeClosed(0, 9).map(key -> server0.underlyingActor().getDatabase().read(key)).sum(),
                IntStream.rangeClosed(10, 19).map(key -> server1.underlyingActor().getDatabase().read(key)).sum()
        );
    }

    @Test
    void sequentialTransactionsWithCoordinatorCrashes() throws InterruptedException {
        coordinator.underlyingActor().getParameters().coordinatorOnVoteRequestCrashProbability = 0.25;
        coordinator.underlyingActor().getParameters().coordinatorOnVoteResponseCrashProbability = 0.25;
        coordinator.underlyingActor().getParameters().coordinatorOnFinalDecisionCrashProbability = 0.25;

        int txnLeft = N_TRANSACTIONS;
        while (txnLeft > 0) {
            MyTestKit testKit = new MyTestKit(system);
            testKit.logger.info(txnLeft + " transactions left");

            var begin = new TxnBeginMsg();
            coordinator.tell(begin, testKit.testActor());

            try {
                testKit.expectMsg(new TxnAcceptMsg(begin.uuid));
            } catch (AssertionError err) {
                testKit.logger.info(testKit.testActor().path().name() + ": timeout while waiting for TxnAcceptMsg");
                Thread.sleep(5000);
                continue;
            }

            int keyFrom = random.nextInt(10);
            int keyTo = 10 + random.nextInt(10);

            var read1 = new ReadMsg(begin.uuid, keyFrom);
            coordinator.tell(read1, testKit.testActor());

            int value1;
            try {
                value1 = testKit.expectMsgClass(ReadResultMsg.class).value;
            } catch (AssertionError err) {
                testKit.logger.info(testKit.testActor().path().name() + ": " + begin.uuid + ": timeout while waiting for ReadResultMsg");
                Thread.sleep(5000);
                continue;
            }

            var read2 = new ReadMsg(begin.uuid, keyTo);
            coordinator.tell(read2, testKit.testActor());

            int value2;
            try {
                value2 = testKit.expectMsgClass(ReadResultMsg.class).value;
            } catch (AssertionError err) {
                testKit.logger.info(testKit.testActor().path().name() + ": " + begin.uuid + ": timeout while waiting for ReadResultMsg");
                Thread.sleep(5000);
                continue;
            }

            var write1 = new WriteMsg(begin.uuid, keyFrom, value1 - 1);
            coordinator.tell(write1, testKit.testActor());
            testKit.expectNoMessage();

            var write2 = new WriteMsg(begin.uuid, keyTo, value2 + 1);
            coordinator.tell(write2, testKit.testActor());
            testKit.expectNoMessage();

            var end = new TxnEndMsg(begin.uuid, true);
            coordinator.tell(end, testKit.testActor());

            boolean commit;
            try {
                commit = testKit.expectMsgClass(TxnResultMsg.class).commit;
            } catch (AssertionError err) {
                testKit.logger.info(testKit.testActor().path().name() + ": " + begin.uuid + ": timeout while waiting for TxnResultMsg");
                Thread.sleep(5000);
                continue;
            }

            testKit.logger.info(testKit.testActor().path().name() + ": Transaction completed! Outcome: " + (commit ? "committed" : "aborted"));

            Assertions.assertEquals(initialSum, calculateSum());

            --txnLeft;
        }

        Assertions.assertEquals(initialSum, calculateSum());
    }

    @Test
    void sequentialTransactionsWithServerCrashes() throws InterruptedException {
        List.of(server0, server1).forEach(server -> {
            server.underlyingActor().getParameters().serverOnVoteResponseCrashProbability = 0.25;
            server.underlyingActor().getParameters().serverOnDecisionRequestCrashProbability = 0.25;
            server.underlyingActor().getParameters().serverOnDecisionResponseCrashProbability = 0.25;
        });

        int txnLeft = N_TRANSACTIONS;
        while (txnLeft > 0) {
            MyTestKit testKit = new MyTestKit(system);
            testKit.logger.info(txnLeft + " transactions left");

            var begin = new TxnBeginMsg();
            coordinator.tell(begin, testKit.testActor());

            try {
                testKit.expectMsg(new TxnAcceptMsg(begin.uuid));
            } catch (AssertionError err) {
                testKit.logger.info(testKit.testActor().path().name() + ": timeout while waiting for TxnAcceptMsg");
                Thread.sleep(5000);
                continue;
            }

            int keyFrom = random.nextInt(10);
            int keyTo = 10 + random.nextInt(10);

            var read1 = new ReadMsg(begin.uuid, keyFrom);
            coordinator.tell(read1, testKit.testActor());

            int value1;
            try {
                value1 = testKit.expectMsgClass(ReadResultMsg.class).value;
            } catch (AssertionError err) {
                testKit.logger.info(testKit.testActor().path().name() + ": " + begin.uuid + ": timeout while waiting for ReadResultMsg");
                Thread.sleep(5000);
                continue;
            }

            var read2 = new ReadMsg(begin.uuid, keyTo);
            coordinator.tell(read2, testKit.testActor());

            int value2;
            try {
                value2 = testKit.expectMsgClass(ReadResultMsg.class).value;
            } catch (AssertionError err) {
                testKit.logger.info(testKit.testActor().path().name() + ": " + begin.uuid + ": timeout while waiting for ReadResultMsg");
                Thread.sleep(5000);
                continue;
            }

            var write1 = new WriteMsg(begin.uuid, keyFrom, value1 - 1);
            coordinator.tell(write1, testKit.testActor());
            testKit.expectNoMessage();

            var write2 = new WriteMsg(begin.uuid, keyTo, value2 + 1);
            coordinator.tell(write2, testKit.testActor());
            testKit.expectNoMessage();

            var end = new TxnEndMsg(begin.uuid, true);
            coordinator.tell(end, testKit.testActor());

            boolean commit;
            try {
                commit = testKit.expectMsgClass(TxnResultMsg.class).commit;
            } catch (AssertionError err) {
                testKit.logger.info(testKit.testActor().path().name() + ": " + begin.uuid + ": timeout while waiting for TxnResultMsg");
                Thread.sleep(5000);
                continue;
            }

            testKit.logger.info(testKit.testActor().path().name() + ": Transaction completed! Outcome: " + (commit ? "committed" : "aborted"));

            Assertions.assertEquals(initialSum, calculateSum());

            --txnLeft;
        }

        Assertions.assertEquals(initialSum, calculateSum());
    }

    @Test
    void sequentialTransactionsWithCoordinatorAndServerCrashes() throws InterruptedException {
        coordinator.underlyingActor().getParameters().coordinatorOnVoteRequestCrashProbability = 0.25;
        coordinator.underlyingActor().getParameters().coordinatorOnVoteResponseCrashProbability = 0.25;
        coordinator.underlyingActor().getParameters().coordinatorOnFinalDecisionCrashProbability = 0.25;

        List.of(server0, server1).forEach(server -> {
            server.underlyingActor().getParameters().serverOnVoteResponseCrashProbability = 0.25;
            server.underlyingActor().getParameters().serverOnDecisionRequestCrashProbability = 0.25;
            server.underlyingActor().getParameters().serverOnDecisionResponseCrashProbability = 0.25;
        });

        int txnLeft = N_TRANSACTIONS;
        while (txnLeft > 0) {
            MyTestKit testKit = new MyTestKit(system);
            testKit.logger.info(txnLeft + " transactions left");

            var begin = new TxnBeginMsg();
            coordinator.tell(begin, testKit.testActor());

            try {
                testKit.expectMsg(new TxnAcceptMsg(begin.uuid));
            } catch (AssertionError err) {
                testKit.logger.info(testKit.testActor().path().name() + ": timeout while waiting for TxnAcceptMsg");
                Thread.sleep(5000);
                continue;
            }

            int keyFrom = random.nextInt(10);
            int keyTo = 10 + random.nextInt(10);

            var read1 = new ReadMsg(begin.uuid, keyFrom);
            coordinator.tell(read1, testKit.testActor());

            int value1;
            try {
                value1 = testKit.expectMsgClass(ReadResultMsg.class).value;
            } catch (AssertionError err) {
                testKit.logger.info(testKit.testActor().path().name() + ": " + begin.uuid + ": timeout while waiting for ReadResultMsg");
                Thread.sleep(5000);
                continue;
            }

            var read2 = new ReadMsg(begin.uuid, keyTo);
            coordinator.tell(read2, testKit.testActor());

            int value2;
            try {
                value2 = testKit.expectMsgClass(ReadResultMsg.class).value;
            } catch (AssertionError err) {
                testKit.logger.info(testKit.testActor().path().name() + ": " + begin.uuid + ": timeout while waiting for ReadResultMsg");
                Thread.sleep(5000);
                continue;
            }

            var write1 = new WriteMsg(begin.uuid, keyFrom, value1 - 1);
            coordinator.tell(write1, testKit.testActor());
            testKit.expectNoMessage();

            var write2 = new WriteMsg(begin.uuid, keyTo, value2 + 1);
            coordinator.tell(write2, testKit.testActor());
            testKit.expectNoMessage();

            var end = new TxnEndMsg(begin.uuid, true);
            coordinator.tell(end, testKit.testActor());

            boolean commit;
            try {
                commit = testKit.expectMsgClass(TxnResultMsg.class).commit;
            } catch (AssertionError err) {
                testKit.logger.info(testKit.testActor().path().name() + ": " + begin.uuid + ": timeout while waiting for TxnResultMsg");
                Thread.sleep(5000);
                continue;
            }

            testKit.logger.info(testKit.testActor().path().name() + ": Transaction completed! Outcome: " + (commit ? "committed" : "aborted"));

            Assertions.assertEquals(initialSum, calculateSum());

            --txnLeft;
        }

        Assertions.assertEquals(initialSum, calculateSum());
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

