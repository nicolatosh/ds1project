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

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class EndToEnd {
    ActorSystem system;
    TestActorRef<Server> server0, server1;
    TestActorRef<Coordinator> coordinator;

    final int initialSum = 20 * DatabaseBuilder.DEFAULT_DATA_VALUE;

    @BeforeEach
    void setUp() {
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
    void sequentialTransactionsWithCoordinatorCrashes() {
        coordinator.underlyingActor().getParameters().coordinatorOnVoteRequestCrashProbability = 0.3;
        coordinator.underlyingActor().getParameters().coordinatorOnVoteResponseCrashProbability = 0.3;
        coordinator.underlyingActor().getParameters().coordinatorOnFinalDecisionCrashProbability = 0.3;

        Random rand = new Random();

        new TestKit(system) {
            {
                for (int txnN = 0; txnN < 20; txnN++) {
                    // begin transaction loop iteration

                    coordinator.tell(new TxnBeginMsg(), testActor());
                    UUID uuid = expectMsgClass(TxnAcceptMsg.class).uuid;

                    int keyFrom = rand.nextInt(20);
                    int keyTo = rand.nextInt(20);
                    while (keyTo == keyFrom) {
                        keyTo = rand.nextInt(20);
                    }

                    ReadMsg read1 = new ReadMsg(uuid, keyFrom);
                    coordinator.tell(read1, testActor());
                    int value1 = expectMsgClass(ReadResultMsg.class).value;

                    ReadMsg read2 = new ReadMsg(uuid, keyTo);
                    coordinator.tell(read2, testActor());
                    int value2 = expectMsgClass(ReadResultMsg.class).value;

                    WriteMsg write1 = new WriteMsg(uuid, keyFrom, value1 - 1);
                    coordinator.tell(write1, testActor());
                    expectNoMessage();

                    WriteMsg write2 = new WriteMsg(uuid, keyTo, value2 + 1);
                    coordinator.tell(write2, testActor());
                    expectNoMessage();

                    TxnEndMsg end = new TxnEndMsg(uuid, true);
                    coordinator.tell(end, testActor());
                    expectMsg(Duration.create(1, TimeUnit.MINUTES), new TxnResultMsg(uuid, true));

                    Assertions.assertEquals(initialSum, calculateSum());

                    // end transaction loop iteration
                }
            }
        };
    }

    @Test
    void sequentialTransactionsWithServerCrashes() {
        List.of(server0, server1).forEach(server -> {
            server.underlyingActor().getParameters().serverOnVoteResponseCrashProbability = 0.3;
            server.underlyingActor().getParameters().serverOnDecisionRequestCrashProbability = 0.3;
            server.underlyingActor().getParameters().serverOnDecisionResponseCrashProbability = 0.3;
        });

        Random rand = new Random();

        new TestKit(system) {
            {
                for (int txnN = 0; txnN < 20; txnN++) {
                    // begin transaction loop iteration

                    coordinator.tell(new TxnBeginMsg(), testActor());
                    UUID uuid = expectMsgClass(TxnAcceptMsg.class).uuid;

                    int keyFrom = rand.nextInt(20);
                    int keyTo = rand.nextInt(20);
                    while (keyTo == keyFrom) {
                        keyTo = rand.nextInt(20);
                    }

                    try {
                        ReadMsg read1 = new ReadMsg(uuid, keyFrom);
                        coordinator.tell(read1, testActor());
                        int value1 = expectMsgClass(Duration.create(15, TimeUnit.SECONDS), ReadResultMsg.class).value;

                        ReadMsg read2 = new ReadMsg(uuid, keyTo);
                        coordinator.tell(read2, testActor());
                        int value2 = expectMsgClass(Duration.create(15, TimeUnit.SECONDS), ReadResultMsg.class).value;

                        WriteMsg write1 = new WriteMsg(uuid, keyFrom, value1 - 1);
                        coordinator.tell(write1, testActor());
                        expectNoMessage();

                        WriteMsg write2 = new WriteMsg(uuid, keyTo, value2 + 1);
                        coordinator.tell(write2, testActor());
                        expectNoMessage();

                        TxnEndMsg end = new TxnEndMsg(uuid, true);
                        coordinator.tell(end, testActor());
                        expectMsg(Duration.create(15, TimeUnit.SECONDS), new TxnResultMsg(uuid, true));

                    } catch (AssertionError err) {
                        TxnEndMsg end = new TxnEndMsg(uuid, false);
                        coordinator.tell(end, testActor());
                        expectMsg(Duration.create(15, TimeUnit.SECONDS), new TxnResultMsg(uuid, false));
                    } finally {
                        // regardless of commit or abort, the sum must be consistent
                        Assertions.assertEquals(initialSum, calculateSum());
                    }

                    // end transaction loop iteration
                }
            }
        };
    }
}