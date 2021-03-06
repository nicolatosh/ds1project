package it.unitn.arpino.ds1project.nodes.server;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.messages.server.DecisionRequest;
import it.unitn.arpino.ds1project.messages.server.ReadRequest;
import it.unitn.arpino.ds1project.messages.server.VoteRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.Duration;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertSame;

public class ServerResumeTest {
    ActorSystem system;
    TestActorRef<Server> server0;

    @BeforeEach
    void setUp() {
        system = ActorSystem.create();
        server0 = TestActorRef.create(system, Server.props(0, 9), "server0");
    }

    @AfterEach
    void tearDown() {
        TestKit.shutdownActorSystem(system, Duration.create(1, TimeUnit.SECONDS), true);
        system = null;
        server0 = null;
    }

    @Test
    void shouldAbortActiveContext() {
        var uuid = UUID.randomUUID();

        var readRequest = new ReadRequest(uuid, 0);
        server0.tell(readRequest, ActorRef.noSender());

        server0.underlyingActor().crash();
        server0.underlyingActor().resume();

        var ctx = server0.underlyingActor().getRepository().getRequestContextById(uuid);

        assertSame(ServerRequestContext.LogState.GLOBAL_ABORT, ctx.loggedState());
    }

    @Test
    void shouldRunTerminationProtocol() {
        new TestKit(system) {
            {
                var uuid = UUID.randomUUID();

                var readRequest = new ReadRequest(uuid, 0);
                server0.tell(readRequest, ActorRef.noSender());

                awaitCond(() -> server0.underlyingActor().getRepository().existsContextWithId(uuid),
                        Duration.create(2, TimeUnit.SECONDS),
                        Duration.create(1, TimeUnit.SECONDS),
                        null);

                var voteRequest = new VoteRequest(uuid, Set.of(server0, testActor()));
                server0.tell(voteRequest, ActorRef.noSender());

                var ctx = server0.underlyingActor().getRepository().getRequestContextById(uuid);
                awaitCond(() -> ServerRequestContext.LogState.VOTE_COMMIT == ctx.loggedState(),
                        Duration.create(ServerRequestContext.CONVERSATIONAL_TIMEOUT + 1, TimeUnit.SECONDS),
                        Duration.create(1, TimeUnit.SECONDS),
                        null);

                server0.underlyingActor().crash();
                server0.underlyingActor().resume();

                var decisionRequest = new DecisionRequest(uuid);
                expectMsg(decisionRequest);
            }
        };
    }
}
