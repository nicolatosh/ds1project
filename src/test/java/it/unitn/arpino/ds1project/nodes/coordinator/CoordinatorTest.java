package it.unitn.arpino.ds1project.nodes.coordinator;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.datastore.database.DatabaseBuilder;
import it.unitn.arpino.ds1project.messages.client.ReadResultMsg;
import it.unitn.arpino.ds1project.messages.client.TxnAcceptMsg;
import it.unitn.arpino.ds1project.messages.client.TxnResultMsg;
import it.unitn.arpino.ds1project.messages.coordinator.ReadMsg;
import it.unitn.arpino.ds1project.messages.coordinator.TxnBeginMsg;
import it.unitn.arpino.ds1project.messages.coordinator.TxnEndMsg;
import it.unitn.arpino.ds1project.messages.coordinator.WriteMsg;
import it.unitn.arpino.ds1project.messages.server.FinalDecision;
import it.unitn.arpino.ds1project.nodes.server.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.Duration;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

public class CoordinatorTest {
    ActorSystem system;
    TestActorRef<Server> server0, server1;
    TestActorRef<Coordinator> coordinator;

    @BeforeEach
    void setUp() {
        system = ActorSystem.create();

        server0 = TestActorRef.create(system, Server.props(0, 9), "server0");
        server1 = TestActorRef.create(system, Server.props(10, 19), "server1");
        server0.underlyingActor().addServer(server1);
        server1.underlyingActor().addServer(server0);

        coordinator = TestActorRef.create(system, Coordinator.props(), "coordinator");
        IntStream.rangeClosed(0, 9).forEach(key -> coordinator.underlyingActor().getDispatcher().map(key, server0));
        IntStream.rangeClosed(10, 19).forEach(key -> coordinator.underlyingActor().getDispatcher().map(key, server1));
    }

    @AfterEach
    void tearDown() {
        TestKit.shutdownActorSystem(system, scala.concurrent.duration.Duration.create(1, TimeUnit.SECONDS), false);
        system = null;
        server0 = null;
        server1 = null;
        coordinator = null;
    }

    @Test
    @Order(0)
    void testNoDuplicateContexts() {
        new TestKit(system) {
            {
                // Note: right now, coordinators can accept two simultaneous transactions from the same client.

                coordinator.tell(new TxnBeginMsg(), testActor());
                UUID uuid1 = expectMsgClass(TxnAcceptMsg.class).uuid;

                coordinator.tell(new TxnBeginMsg(), testActor());
                UUID uuid2 = expectMsgClass(TxnAcceptMsg.class).uuid;

                assertNotEquals(uuid1, uuid2);

                List<CoordinatorRequestContext> contexts = coordinator.underlyingActor().getActive();
                assertEquals(2, contexts.size());
                assertNotEquals(contexts.get(0), contexts.get(1));
            }
        };
    }

    @Test
    @Order(1)
    void testNoDuplicateParticipant() {
        CoordinatorRequestContext ctx = new CoordinatorRequestContext(UUID.randomUUID(), ActorRef.noSender());
        coordinator.underlyingActor().addContext(ctx);

        coordinator.tell(new ReadMsg(ctx.uuid, 0), ActorRef.noSender());
        coordinator.tell(new ReadMsg(ctx.uuid, 0), ActorRef.noSender());
        assertEquals(1, ctx.getParticipants().size());
    }

    @Test
    @Order(2)
    void testFinalDecisionTimeout() {
        new TestKit(system) {
            {
                coordinator.underlyingActor().getDispatcher().map(0, testActor());

                CoordinatorRequestContext ctx = new CoordinatorRequestContext(UUID.randomUUID(), testActor());
                coordinator.underlyingActor().addContext(ctx);
                ctx.addParticipant(testActor());

                ctx.startVoteResponseTimeout(coordinator.underlyingActor());
                // must account for the timeout duration to elapse and the message to be delivered:
                // add one second more to the duration
                expectMsg(Duration.create(CoordinatorRequestContext.VOTE_RESPONSE_TIMEOUT_S + 1, TimeUnit.SECONDS),
                        new FinalDecision(ctx.uuid, FinalDecision.Decision.GLOBAL_ABORT));
                assertSame(CoordinatorRequestContext.TwoPhaseCommitFSM.ABORT, ctx.getProtocolState());
            }
        };
    }

    @Test
    @Order(3)
    void testConcurrentTxn() {
        new TestKit(system) {
            {
                // Mock the clients

                ActorRef client1 = testActor();

                var testKit2 = new TestKit(system);
                ActorRef client2 = testKit2.testActor();

                // The clients begin two transactions.

                coordinator.tell(new TxnBeginMsg(), client1);
                TxnAcceptMsg accept1 = expectMsgClass(TxnAcceptMsg.class);
                UUID uuid1 = accept1.uuid;

                coordinator.tell(new TxnBeginMsg(), client2);
                TxnAcceptMsg accept2 = testKit2.expectMsgClass(TxnAcceptMsg.class);
                UUID uuid2 = accept2.uuid;

                // The first transaction writes two elements, stored in two different servers,
                // and subsequently reads them.

                coordinator.tell(new WriteMsg(uuid1, 4, 40), client1);
                expectNoMessage();

                coordinator.tell(new WriteMsg(uuid1, 17, 170), client1);
                expectNoMessage();

                coordinator.tell(new ReadMsg(uuid1, 4), client1);
                expectMsg(new ReadResultMsg(uuid1, 4, 40));

                coordinator.tell(new ReadMsg(uuid1, 17), client1);
                expectMsg(new ReadResultMsg(uuid1, 17, 170));

                // The second transaction reads two elements, one of which was written by the first transaction.
                // Since the first transaction has not yet committed, both the read values must be different from
                // the values read by the first transaction.

                coordinator.tell(new ReadMsg(uuid2, 4), client2);
                testKit2.expectMsg(new ReadResultMsg(uuid2, 4, DatabaseBuilder.DEFAULT_DATA_VALUE));

                coordinator.tell(new ReadMsg(uuid2, 13), client2);
                testKit2.expectMsg(new ReadResultMsg(uuid2, 13, DatabaseBuilder.DEFAULT_DATA_VALUE));

                // The first transaction commits. The response from the server must be positive.

                coordinator.tell(new TxnEndMsg(uuid1, true), client1);
                expectMsg(new TxnResultMsg(uuid1, true));

                // The second transaction commits. The response from the server must be negative.

                coordinator.tell(new TxnEndMsg(uuid2, true), client2);
                testKit2.expectMsg(new TxnResultMsg(uuid2, false));
            }
        };
    }
}
