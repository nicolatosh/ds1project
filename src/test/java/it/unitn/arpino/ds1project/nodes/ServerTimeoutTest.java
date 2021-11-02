package it.unitn.arpino.ds1project.nodes;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.messages.ServerInfo;
import it.unitn.arpino.ds1project.messages.TimeoutExpired;
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

public class ServerTimeoutTest {
    ActorSystem system;
    TestActorRef<Server> server;
    TestActorRef<Server> server2;

    @BeforeEach
    void setUp() {
        system = ActorSystem.create();
        server = TestActorRef.create(system, Server.props(0, 9), "server");
        server2 = TestActorRef.create(system, Server.props(20, 29), "server2");
    }

    @AfterEach
    void tearDown() {
        TestKit.shutdownActorSystem(system, scala.concurrent.duration.Duration.create(1, TimeUnit.SECONDS), false);
        system = null;
        server = null;
        server2 = null;
    }

    @Test
    void testTimeout() {
        new TestKit(system) {
            {
                // Mock the coordinator

                ActorRef coord = testActor();

                // Updating servers knowledge of each other

                ServerInfo info1 = new ServerInfo(server, 0, 9);
                ServerInfo info2 = new ServerInfo(server2, 20, 29);

                server.tell(info2, ActorRef.noSender());
                server2.tell(info1, ActorRef.noSender());

                // Trasaction affecting 2 servers

                UUID uuid1 = UUID.randomUUID();
                WriteRequest write1 = new WriteRequest(uuid1, 6, 742);
                server.tell(write1, coord);
                expectNoMessage();

                WriteRequest write2 = new WriteRequest(uuid1, 26, 7);
                server2.tell(write2, coord);
                expectNoMessage();

                ReadRequest read1 = new ReadRequest(uuid1, 6);
                server.tell(read1, coord);
                ReadResult result1 = expectMsgClass(ReadResult.class);
                Assertions.assertEquals(742, result1.value);

                VoteRequest voteRequest1 = new VoteRequest(uuid1);
                server.tell(voteRequest1, coord);
                VoteResponse voteResponse1 = expectMsgClass(VoteResponse.class);
                assertEquals(Vote.YES, voteResponse1.vote);

                VoteRequest voteRequest2 = new VoteRequest(uuid1);
                server2.tell(voteRequest2, coord);
                VoteResponse voteResponse2 = expectMsgClass(VoteResponse.class);
                assertEquals(Vote.YES, voteResponse2.vote);

                // Coordinator sends decision to Server2
                FinalDecision decision = new FinalDecision(uuid1, FinalDecision.Decision.GLOBAL_COMMIT);
                server2.tell(decision, coord);
                expectNoMessage();

                // Server1 does not receive the final decision from coordinator
                TimeoutExpired timeout = new TimeoutExpired(uuid1);
                server.tell(timeout, server);
                expectNoMessage();


            }
        };
    }
}
