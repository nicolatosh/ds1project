package it.unitn.arpino.ds1project.nodes.server;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.messages.server.FinalDecision;
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

public class ServerOnFinalDecisionTest {
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
    void testFinalDecisionOnInit() {
        var uuid = UUID.randomUUID();

        var readRequest = new ReadRequest(uuid, 0);
        server0.tell(readRequest, ActorRef.noSender());

        var decision = new FinalDecision(uuid, FinalDecision.Decision.GLOBAL_ABORT);
        server0.tell(decision, ActorRef.noSender());

        var ctx = server0.underlyingActor().getRepository().getRequestContextById(uuid);

        assertSame(ServerRequestContext.LogState.GLOBAL_ABORT, ctx.loggedState());
    }

    @Test
    void testFinalDecisionOnVoteCommit() {
        var uuid = UUID.randomUUID();

        var readRequest = new ReadRequest(uuid, 0);
        server0.tell(readRequest, ActorRef.noSender());

        var voteRequest = new VoteRequest(uuid, Set.of(server0));
        server0.tell(voteRequest, ActorRef.noSender());

        var decision = new FinalDecision(uuid, FinalDecision.Decision.GLOBAL_ABORT);
        server0.tell(decision, ActorRef.noSender());

        var ctx = server0.underlyingActor().getRepository().getRequestContextById(uuid);

        assertSame(ServerRequestContext.LogState.GLOBAL_ABORT, ctx.loggedState());
    }
}
