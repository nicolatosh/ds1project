package it.unitn.arpino.ds1project.nodes.server;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.datastore.database.DatabaseBuilder;
import it.unitn.arpino.ds1project.messages.StartMessage;
import it.unitn.arpino.ds1project.messages.coordinator.ReadResult;
import it.unitn.arpino.ds1project.messages.coordinator.VoteResponse;
import it.unitn.arpino.ds1project.messages.server.FinalDecision;
import it.unitn.arpino.ds1project.messages.server.ReadRequest;
import it.unitn.arpino.ds1project.messages.server.VoteRequest;
import it.unitn.arpino.ds1project.messages.server.WriteRequest;
import it.unitn.arpino.ds1project.nodes.DataStoreNode;
import org.junit.jupiter.api.*;
import scala.concurrent.duration.Duration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertSame;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ServerTransactionTest {
    ActorSystem system;
    TestActorRef<Server> server;

    @BeforeEach
    void setUp() {
        system = ActorSystem.create();
        server = TestActorRef.create(system, Server.props(0, 9), "server");

        server.tell(new StartMessage(), ActorRef.noSender());
    }

    @AfterEach
    void tearDown() {
        TestKit.shutdownActorSystem(system, Duration.create(1, TimeUnit.SECONDS), true);
        system = null;
        server = null;
    }

    @Test
    @Order(1)
    void testVoteRequestTimeout() throws InterruptedException {
        new TestKit(system) {
            {
                UUID uuid = UUID.randomUUID();
                server.tell(new ReadRequest(uuid, 0), testActor());
                expectMsg(new ReadResult(uuid, 0, DatabaseBuilder.DEFAULT_DATA_VALUE));

                TimeUnit.SECONDS.sleep(ServerRequestContext.VOTE_REQUEST_TIMEOUT_S + 1);

                server.tell(new VoteRequest(uuid), testActor());
                assertSame(DataStoreNode.Status.ALIVE, server.underlyingActor().getStatus());
                assertSame(ServerRequestContext.TwoPhaseCommitFSM.ABORT, server.underlyingActor().getRequestContext(uuid).orElseThrow().getProtocolState());
            }
        };
    }

    @Test
    @Order(2)
    void testConcurrentTxn() {
        new TestKit(system) {
            {
                // Mock the first transaction, mocking the messages from a coordinator.
                // The transaction writes a value, which affects the transaction's own private workspace.

                UUID uuid1 = UUID.randomUUID();
                server.tell(new WriteRequest(uuid1, 6, 742), testActor());
                expectNoMessage();

                server.tell(new ReadRequest(uuid1, 6), testActor());
                expectMsg(new ReadResult(uuid1, 6, 742));

                // Mock the first transaction, mocking the messages from a coordinator.
                // The transaction does a read of the same element written by the first transaction.
                // The read will be done on the second transaction's own private workspace.
                // As the first transaction has not yet committed, the read value must not be the value
                // written by the first transaction.

                UUID uuid2 = UUID.randomUUID();
                server.tell(new ReadRequest(uuid2, 6), testActor());
                expectMsg(new ReadResult(uuid2, 6, DatabaseBuilder.DEFAULT_DATA_VALUE));

                // The first transaction commits. The value in the transaction's private workspace
                // is copied into the database.

                server.tell(new VoteRequest(uuid1), testActor());
                expectMsg(new VoteResponse(uuid1, VoteResponse.Vote.YES));

                FinalDecision decision1 = new FinalDecision(uuid1, FinalDecision.Decision.GLOBAL_COMMIT);
                server.tell(decision1, testActor());
                expectNoMessage();

                // The second transaction must not perceive the change introduced by the commit:
                // it is still working on its own private workspace.

                server.tell(new ReadRequest(uuid2, 6), testActor());
                expectMsg(new ReadResult(uuid2, 6, DatabaseBuilder.DEFAULT_DATA_VALUE));

                // Mock the third transaction, mocking the messages from a coordinator.
                // The transaction does a read of the same element written by the first transaction.
                // In this case, the read value must be the committed value.

                UUID uuid3 = UUID.randomUUID();
                server.tell(new ReadRequest(uuid3, 6), testActor());
                expectMsg(new ReadResult(uuid3, 6, 742));

                // The second transaction attempts commits. The response must be negative.

                server.tell(new VoteRequest(uuid2), testActor());
                expectMsg(new VoteResponse(uuid2, VoteResponse.Vote.NO));

                server.tell(new FinalDecision(uuid2, FinalDecision.Decision.GLOBAL_ABORT), testActor());
                expectNoMessage();
            }
        };
    }
}
