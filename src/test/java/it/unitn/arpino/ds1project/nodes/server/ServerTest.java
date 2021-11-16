package it.unitn.arpino.ds1project.nodes.server;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.messages.server.VoteRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.Duration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

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
        TestKit.shutdownActorSystem(system, Duration.create(1, TimeUnit.SECONDS), true);
        system = null;
        server = null;
    }

    @Test
    @Order(1)
    void testVoteRequestTimeout() throws InterruptedException {
        new TestKit(system) {
            {
                ServerRequestContext ctx = server.underlyingActor().createNewContext(UUID.randomUUID());
                ctx.log(ServerRequestContext.LogState.INIT);
                ctx.startVoteRequestTimer(server.underlyingActor());

                TimeUnit.SECONDS.sleep(ServerRequestContext.VOTE_REQUEST_TIMEOUT_S + 1);

                assertSame(ServerRequestContext.LogState.GLOBAL_ABORT, ctx.loggedState());
                assertSame(ServerRequestContext.TwoPhaseCommitFSM.ABORT, ctx.getProtocolState());
            }
        };
    }

    @Test
    @Order(2)
    void testFinalDecisionTimeout() throws InterruptedException {
        new TestKit(system) {
            {
                ServerRequestContext ctx = server.underlyingActor().createNewContext(UUID.randomUUID());
                ctx.log(ServerRequestContext.LogState.INIT);
                ctx.startVoteRequestTimer(server.underlyingActor());

                server.tell(new VoteRequest(ctx.uuid), ActorRef.noSender());

                TimeUnit.SECONDS.sleep(ServerRequestContext.FINAL_DECISION_TIMEOUT_S + 1);

                assertFalse(ctx.isDecided());
            }
        };
    }
}
