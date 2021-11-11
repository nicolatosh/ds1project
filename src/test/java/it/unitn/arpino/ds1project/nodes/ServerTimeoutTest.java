package it.unitn.arpino.ds1project.nodes;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.messages.coordinator.ReadResult;
import it.unitn.arpino.ds1project.messages.coordinator.VoteResponse;
import it.unitn.arpino.ds1project.messages.server.*;
import it.unitn.arpino.ds1project.nodes.server.Server;
import it.unitn.arpino.ds1project.nodes.server.ServerRequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertSame;

public class ServerTimeoutTest {
    ActorSystem system;
    TestActorRef<Server> server1;

    @BeforeEach
    void setUp() {
        system = ActorSystem.create();
        server1 = TestActorRef.create(system, Server.props(0, 9), "server");
    }

    @AfterEach
    void tearDown() {
        TestKit.shutdownActorSystem(system, scala.concurrent.duration.Duration.create(1, TimeUnit.SECONDS), false);
        system = null;
        server1 = null;
    }

    @Test
    void testTimeout() {
        new TestKit(system) {
            {
                // Mock the coordinator and the second server.

                ActorRef coord = testActor();

                TestKit testKit2 = new TestKit(system);
                ActorRef server2 = testKit2.testActor();

                // Update server1's knowledge of server2
                server1.underlyingActor().addServer(server2);

                // Simulate a transaction by requesting two operations: a WriteRequest and a ReadRequest.

                UUID uuid = UUID.randomUUID();

                server1.tell(new WriteRequest(uuid, 6, 742), coord);
                expectNoMessage();

                server1.tell(new ReadRequest(uuid, 6), coord);
                expectMsg(new ReadResult(uuid, 6, 742));

                // Send a VoteRequest to server1. The ServerRequestContext must switch to the READY state.
                // server1 starts a timer within which to receive the FinalDecision from the coordinator.

                VoteRequest voteRequest = new VoteRequest(uuid);
                server1.tell(voteRequest, coord);
                assertSame(ServerRequestContext.TwoPhaseCommitFSM.READY,
                        server1.underlyingActor().getRequestContext(voteRequest).orElseThrow().getProtocolState());

                expectMsg(new VoteResponse(uuid, VoteResponse.Vote.YES));

                // The timeout expires, and server1 sends a TimeoutExpire message to itself, upon which it asks server2
                // if it knows about the FinalDecision.

                testKit2.expectMsgClass(DecisionRequest.class);

                // Suppose that server2 knows the FinalDecision: it sends it to server1.

                FinalDecision decision = new FinalDecision(uuid, FinalDecision.Decision.GLOBAL_COMMIT);
                server1.tell(decision, server2);
                expectNoMessage();

                // The ServerRequestContext must switch to the COMMIT state.

                ServerRequestContext ctx = server1.underlyingActor().getRequestContext(decision).orElseThrow();
                assertSame(ServerRequestContext.TwoPhaseCommitFSM.COMMIT, ctx.getProtocolState());
            }
        };
    }
}
