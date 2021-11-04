package it.unitn.arpino.ds1project.nodes;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.messages.ServerInfo;
import it.unitn.arpino.ds1project.messages.TimeoutExpired;
import it.unitn.arpino.ds1project.messages.coordinator.ReadResult;
import it.unitn.arpino.ds1project.messages.coordinator.VoteResponse;
import it.unitn.arpino.ds1project.messages.server.*;
import it.unitn.arpino.ds1project.nodes.server.Server;
import it.unitn.arpino.ds1project.nodes.server.ServerRequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ServerTimeoutTest {
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
    void testTimeout() {
        new TestKit(system) {
            {
                // Mock the coordinator and the second server
                ActorRef coord = testActor();
                ActorRef server2 = testActor();

                // Updating server knowledge of the other
                ServerInfo info2 = new ServerInfo(server2, 20, 29);
                server.tell(info2, ActorRef.noSender());

                // Trasaction affecting 2 servers
                // Server2 read/writes are emulated

                UUID uuid1 = UUID.randomUUID();
                WriteRequest write1 = new WriteRequest(uuid1, 6, 742);
                server.tell(write1, coord);
                expectNoMessage();

                ReadRequest read1 = new ReadRequest(uuid1, 6);
                server.tell(read1, coord);
                ReadResult result1 = expectMsgClass(ReadResult.class);
                Assertions.assertEquals(742, result1.value);

                // Checking that read and write requests are part of the same transaction

                Server server_actor = server.underlyingActor();
                ServerRequestContext ctx = server_actor.getRequestContext(read1).orElseThrow();
                ServerRequestContext ctx2 = server_actor.getRequestContext(write1).orElseThrow();
                assertEquals(ctx, ctx2);


                VoteRequest voteRequest1 = new VoteRequest(uuid1);
                server.tell(voteRequest1, coord);
                VoteResponse voteResponse1 = expectMsgClass(VoteResponse.class);
                assertEquals(VoteResponse.Vote.YES, voteResponse1.vote);

                // Coordinator sends decision to server2 but not to server1
                FinalDecision decision = new FinalDecision(uuid1, FinalDecision.Decision.GLOBAL_COMMIT);
                server2.tell(decision, coord);
                expectMsgClass(FinalDecision.class);

                // Server1 does not receive the final decision from coordinator
                // DecisionRequest issued
                TimeoutExpired timeout = new TimeoutExpired(uuid1);
                server.tell(timeout, server);
                expectMsgClass(DecisionRequest.class);

                // Suppose that Server2 do not know the decision
                DecisionResponse response = new DecisionResponse(uuid1, ServerRequestContext.TwoPhaseCommitFSM.READY);
                server.tell(response, server2);
                expectNoMessage();


                Server server_actor2 = server.underlyingActor();
                ServerRequestContext ctx_response = server_actor2.getRequestContext(response).orElseThrow();
                ServerRequestContext.TwoPhaseCommitFSM state = ctx_response.getProtocolState();

                // Server must be still in Ready state since the other server did not know the decision
                assertEquals(state, ServerRequestContext.TwoPhaseCommitFSM.READY);

            }
        };
    }
}
