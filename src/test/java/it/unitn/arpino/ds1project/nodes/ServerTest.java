package it.unitn.arpino.ds1project.nodes;

import akka.actor.ActorSystem;
import akka.pattern.Patterns;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.datastore.DatabaseBuilder;
import it.unitn.arpino.ds1project.messages.coordinator.ReadResult;
import it.unitn.arpino.ds1project.messages.coordinator.VoteResponse;
import it.unitn.arpino.ds1project.messages.server.FinalDecision;
import it.unitn.arpino.ds1project.messages.server.ReadRequest;
import it.unitn.arpino.ds1project.messages.server.VoteRequest;
import it.unitn.arpino.ds1project.messages.server.WriteRequest;
import it.unitn.arpino.ds1project.nodes.coordinator.Decision;
import it.unitn.arpino.ds1project.nodes.server.Server;
import it.unitn.arpino.ds1project.nodes.server.Vote;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class ServerTest {
    ActorSystem system;
    TestActorRef<Server> server;
    CompletableFuture<Object> future;

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
    void testReadSync() throws ExecutionException, InterruptedException {
        UUID uuid = UUID.randomUUID();
        ReadRequest read = new ReadRequest(uuid, 1);

        future = Patterns.ask(server, read, Duration.ofSeconds(1)).toCompletableFuture();
        assertTrue(future.get() instanceof ReadResult);
        ReadResult result = (ReadResult) future.get();
        Assertions.assertEquals(1, result.key);
        Assertions.assertEquals(DatabaseBuilder.DEFAULT_DATA_VALUE, result.value);
    }

    @Test
    void testTransactions() throws ExecutionException, InterruptedException {
        // Mock the first transaction, mocking the messages from a coordinator.
        // The transaction writes a value, which affects the transaction's own private workspace.

        UUID uuid1 = UUID.randomUUID();
        WriteRequest write1 = new WriteRequest(uuid1, 6, 742);
        future = Patterns.ask(server, write1, Duration.ofSeconds(1)).toCompletableFuture();
        future.getNow(null);

        ReadRequest read1 = new ReadRequest(uuid1, 6);
        future = Patterns.ask(server, read1, Duration.ofSeconds(1)).toCompletableFuture();
        assertTrue(future.get() instanceof ReadResult);
        ReadResult result1 = (ReadResult) future.get();
        Assertions.assertEquals(742, result1.value);

        // Mock the first transaction, mocking the messages from a coordinator.
        // The transaction does a read of the same element written by the first transaction.
        // The read will be done on the second transaction's own private workspace.
        // As the first transaction has not yet committed, the read value must not be the value
        // written by the first transaction.

        UUID uuid2 = UUID.randomUUID();
        ReadRequest read2 = new ReadRequest(uuid2, 6);
        future = Patterns.ask(server, read2, Duration.ofSeconds(1)).toCompletableFuture();
        assertTrue(future.get() instanceof ReadResult);
        ReadResult result2 = (ReadResult) future.get();
        assertNotEquals(742, result2.value);
        assertEquals(DatabaseBuilder.DEFAULT_DATA_VALUE, result2.value);

        // The first transaction commits. The value in the transaction's private workspace
        // is copied into the database.

        VoteRequest voteRequest1 = new VoteRequest(uuid1);
        future = Patterns.ask(server, voteRequest1, Duration.ofSeconds(1)).toCompletableFuture();
        assertTrue(future.get() instanceof VoteResponse);
        VoteResponse voteResponse1 = (VoteResponse) future.get();
        assertEquals(Vote.YES, voteResponse1.vote);

        FinalDecision decision1 = new FinalDecision(uuid1, Decision.GLOBAL_COMMIT);
        future = Patterns.ask(server, decision1, Duration.ofSeconds(1)).toCompletableFuture();
        future.getNow(null);

        // The second transaction must not perceive the change introduced by the commit:
        // it is still working on its own private workspace.

        future = Patterns.ask(server, read2, Duration.ofSeconds(1)).toCompletableFuture();
        assertTrue(future.get() instanceof ReadResult);
        result2 = (ReadResult) future.get();
        assertNotEquals(742, result2.value);
        assertEquals(DatabaseBuilder.DEFAULT_DATA_VALUE, result2.value);

        // Mock the third transaction, mocking the messages from a coordinator.
        // The transaction does a read of the same element written by the first transaction.
        // In this case, the read value must be the committed value.

        UUID uuid3 = UUID.randomUUID();
        ReadRequest read3 = new ReadRequest(uuid3, 6);
        future = Patterns.ask(server, read3, Duration.ofSeconds(1)).toCompletableFuture();
        assertTrue(future.get() instanceof ReadResult);
        ReadResult result3 = (ReadResult) future.get();
        assertEquals(742, result3.value);

        // The second transaction attempts commits. The response must be negative.

        VoteRequest voteRequest2 = new VoteRequest(uuid2);
        future = Patterns.ask(server, voteRequest2, Duration.ofSeconds(1)).toCompletableFuture();
        assertTrue(future.get() instanceof VoteResponse);
        VoteResponse voteResponse2 = (VoteResponse) future.get();
        assertEquals(Vote.NO, voteResponse2.vote);

        FinalDecision decision2 = new FinalDecision(uuid2, Decision.GLOBAL_ABORT);
        future = Patterns.ask(server, decision2, Duration.ofSeconds(1)).toCompletableFuture();
        future.getNow(null);
    }
}
