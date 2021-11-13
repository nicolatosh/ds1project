package it.unitn.arpino.ds1project.nodes;

import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.messages.coordinator.ReadResult;
import it.unitn.arpino.ds1project.messages.server.ReadRequest;
import it.unitn.arpino.ds1project.messages.server.VoteRequest;
import it.unitn.arpino.ds1project.nodes.server.Server;
import it.unitn.arpino.ds1project.nodes.server.ServerRequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.Duration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertSame;

public class ServerInitTimeoutTest {

    ActorSystem system;
    TestActorRef<Server> server;

    @BeforeEach
    void setUp() {
        system = ActorSystem.create();
        server = TestActorRef.create(system, Server.props(0, 9), "server");
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
                expectMsgClass(ReadResult.class);

                TimeUnit.SECONDS.sleep(ServerRequestContext.VOTE_REQUEST_TIMEOUT_S + 1);

                server.tell(new VoteRequest(uuid), testActor());
                assertSame(server.underlyingActor().getStatus(), DataStoreNode.Status.ALIVE);
                assertSame(server.underlyingActor().getRequestContext(uuid).get().getProtocolState(), ServerRequestContext.TwoPhaseCommitFSM.ABORT);

            }
        };
    }
}
