package it.unitn.arpino.ds1project.nodes;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.datastore.DatabaseBuilder;
import it.unitn.arpino.ds1project.messages.coordinator.ReadResult;
import it.unitn.arpino.ds1project.messages.coordinator.VoteResponse;
import it.unitn.arpino.ds1project.messages.server.FinalDecision;
import it.unitn.arpino.ds1project.messages.server.ReadRequest;
import it.unitn.arpino.ds1project.messages.server.VoteRequest;
import it.unitn.arpino.ds1project.messages.server.WriteRequest;
import it.unitn.arpino.ds1project.nodes.server.Server;
import it.unitn.arpino.ds1project.nodes.server.Vote;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class ServerTest {
    ActorSystem system;
    TestActorRef<Server> server;

    @BeforeEach
    void setUp() {
        system = ActorSystem.create();
        server = TestActorRef.create(system, Server.props(0, 9), "server");
    }

    @AfterEach
    void tearDown() {
        TestKit.shutdownActorSystem(system, scala.concurrent.duration.Duration.create(1, TimeUnit.SECONDS), false);
        system = null;
        server = null;
    }

    @Test
    void testConcurrentTxn() {
        new TestKit(system) {
            {
                // Mock the coordinator

                ActorRef coord = testActor();

                // Mock the first transaction, mocking the messages from a coordinator.
                // The transaction writes a value, which affects the transaction's own private workspace.

                UUID uuid1 = UUID.randomUUID();
                WriteRequest write1 = new WriteRequest(uuid1, 6, 742);
                server.tell(write1, coord);
                expectNoMessage();

                ReadRequest read1 = new ReadRequest(uuid1, 6);
                server.tell(read1, coord);
                ReadResult result1 = expectMsgClass(ReadResult.class);
                Assertions.assertEquals(742, result1.value);

                // Mock the first transaction, mocking the messages from a coordinator.
                // The transaction does a read of the same element written by the first transaction.
                // The read will be done on the second transaction's own private workspace.
                // As the first transaction has not yet committed, the read value must not be the value
                // written by the first transaction.

                UUID uuid2 = UUID.randomUUID();
                ReadRequest read2 = new ReadRequest(uuid2, 6);
                server.tell(read2, coord);
                ReadResult result2 = expectMsgClass(ReadResult.class);
                assertNotEquals(742, result2.value);
                assertEquals(DatabaseBuilder.DEFAULT_DATA_VALUE, result2.value);

                // The first transaction commits. The value in the transaction's private workspace
                // is copied into the database.

                VoteRequest voteRequest1 = new VoteRequest(uuid1);
                server.tell(voteRequest1, coord);
                VoteResponse voteResponse1 = expectMsgClass(VoteResponse.class);
                assertEquals(Vote.YES, voteResponse1.vote);

                FinalDecision decision1 = new FinalDecision(uuid1, FinalDecision.Decision.GLOBAL_COMMIT);
                server.tell(decision1, coord);
                expectNoMessage();

                // The second transaction must not perceive the change introduced by the commit:
                // it is still working on its own private workspace.

                server.tell(read2, coord);
                result2 = expectMsgClass(ReadResult.class);
                assertNotEquals(742, result2.value);
                assertEquals(DatabaseBuilder.DEFAULT_DATA_VALUE, result2.value);

                // Mock the third transaction, mocking the messages from a coordinator.
                // The transaction does a read of the same element written by the first transaction.
                // In this case, the read value must be the committed value.

                UUID uuid3 = UUID.randomUUID();
                ReadRequest read3 = new ReadRequest(uuid3, 6);
                server.tell(read3, coord);
                ReadResult result3 = expectMsgClass(ReadResult.class);
                assertEquals(742, result3.value);

                // The second transaction attempts commits. The response must be negative.

                VoteRequest voteRequest2 = new VoteRequest(uuid2);
                server.tell(voteRequest2, coord);
                VoteResponse voteResponse2 = expectMsgClass(VoteResponse.class);
                assertEquals(Vote.NO, voteResponse2.vote);

                FinalDecision decision2 = new FinalDecision(uuid2, FinalDecision.Decision.GLOBAL_ABORT);
                server.tell(decision2, coord);
                expectNoMessage();
            }
        };
    }
}
