package it.unitn.arpino.ds1project.nodes.client;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.Duration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ClientTransactionTest {
    ActorSystem system;
    TestActorRef<Server> server0, server1;
    TestActorRef<Coordinator> coordinator;

    @BeforeEach
    void setUp() {
        system = ActorSystem.create();

        server0 = TestActorRef.create(system, Server.props(0, 9), "server0");
        server1 = TestActorRef.create(system, Server.props(10, 19), "server1");
        server0.tell(new JoinMessage(10, 19), server1);
        server1.tell(new JoinMessage(0, 9), server0);

        coordinator = TestActorRef.create(system, Coordinator.props(), "coordinator");
        coordinator.tell(new JoinMessage(0, 9), server0);
        coordinator.tell(new JoinMessage(10, 19), server1);

        server0.tell(new StartMessage(), ActorRef.noSender());
        server0.tell(new StartMessage(), ActorRef.noSender());
        coordinator.tell(new StartMessage(), ActorRef.noSender());
    }

    @AfterEach
    void tearDown() {
        TestKit.shutdownActorSystem(system, Duration.create(1, TimeUnit.SECONDS), true);
        system = null;
        server0 = null;
        server1 = null;
        coordinator = null;
    }

    @Test
    @Order(1)
    void testReadYourWrites() {
        new TestKit(system) {
            {
                coordinator.tell(new TxnBeginMsg(UUID.randomUUID()), testActor());
                UUID uuid = expectMsgClass(TxnAcceptMsg.class).uuid;

                coordinator.tell(new WriteMsg(uuid, 1, 10), testActor());
                expectNoMessage();

                coordinator.tell(new ReadMsg(uuid, 1), testActor());
                expectMsg(new ReadResultMsg(uuid, 1, 10));
            }
        };
    }

    @Test
    @Order(2)
    void testCommit() {
        new TestKit(system) {
            {
                coordinator.tell(new TxnBeginMsg(UUID.randomUUID()), testActor());
                UUID uuid = expectMsgClass(TxnAcceptMsg.class).uuid;

                coordinator.tell(new WriteMsg(uuid, 3, 30), testActor());

                coordinator.tell(new TxnEndMsg(uuid, true), testActor());
                expectMsg(new TxnResultMsg(uuid, true));
            }
        };
    }

    @Test
    @Order(3)
    void testAbort() {
        new TestKit(system) {
            {
                coordinator.tell(new TxnBeginMsg(UUID.randomUUID()), testActor());
                UUID uuid = expectMsgClass(TxnAcceptMsg.class).uuid;

                coordinator.tell(new WriteMsg(uuid, 3, 30), testActor());

                coordinator.tell(new TxnEndMsg(uuid, false), testActor());
                expectMsg(new TxnResultMsg(uuid, false));
            }
        };
    }

    @Test
    @Order(4)
    void testTwoTransactionsWithPositiveOutcome() {
        new TestKit(system) {
            {
                coordinator.tell(new TxnBeginMsg(UUID.randomUUID()), testActor());
                UUID uuid1 = expectMsgClass(TxnAcceptMsg.class).uuid;

                coordinator.tell(new TxnBeginMsg(UUID.randomUUID()), testActor());
                UUID uuid2 = expectMsgClass(TxnAcceptMsg.class).uuid;

                // The two data items are different, thus, the requests to commit should be satisfied.

                coordinator.tell(new WriteMsg(uuid1, 4, 40), testActor());
                coordinator.tell(new WriteMsg(uuid2, 8, 80), testActor());

                coordinator.tell(new TxnEndMsg(uuid1, true), testActor());
                expectMsg(new TxnResultMsg(uuid1, true));

                coordinator.tell(new TxnEndMsg(uuid2, true), testActor());
                expectMsg(new TxnResultMsg(uuid2, true));
            }
        };
    }

    @Test
    @Order(5)
    void testTwoTransactionsWithDifferentOutcome() {
        new TestKit(system) {
            {
                coordinator.tell(new TxnBeginMsg(UUID.randomUUID()), testActor());
                UUID uuid1 = expectMsgClass(TxnAcceptMsg.class).uuid;

                coordinator.tell(new TxnBeginMsg(UUID.randomUUID()), testActor());
                UUID uuid2 = expectMsgClass(TxnAcceptMsg.class).uuid;

                // The two data items are the same, thus, only one request to commit should be satisfied.

                coordinator.tell(new WriteMsg(uuid1, 4, 40), testActor());
                coordinator.tell(new WriteMsg(uuid2, 4, 50), testActor());

                coordinator.tell(new TxnEndMsg(uuid1, true), testActor());
                expectMsg(new TxnResultMsg(uuid1, true));

                coordinator.tell(new TxnEndMsg(uuid2, false), testActor());
                expectMsg(new TxnResultMsg(uuid2, false));
            }
        };
    }
}
