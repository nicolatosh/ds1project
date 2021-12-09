package it.unitn.arpino.ds1project.nodes.server;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.datastore.database.DatabaseBuilder;
import it.unitn.arpino.ds1project.messages.JoinMessage;
import it.unitn.arpino.ds1project.messages.StartMessage;
import it.unitn.arpino.ds1project.messages.coordinator.ReadResult;
import it.unitn.arpino.ds1project.messages.server.DecisionRequest;
import it.unitn.arpino.ds1project.messages.server.ReadRequest;
import it.unitn.arpino.ds1project.messages.server.VoteRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.Duration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ServerTimeoutTest {
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
    void testVoteRequestTimeout() throws InterruptedException {
        new TestKit(system) {
            {
                var start = new StartMessage();
                server0.tell(start, ActorRef.noSender());

                var uuid = UUID.randomUUID();

                var readRequest = new ReadRequest(uuid, 0);
                server0.tell(readRequest, testActor());

                var readResult = new ReadResult(uuid, 0, DatabaseBuilder.DEFAULT_DATA_VALUE);
                expectMsg(readResult);

                var ctx = server0.underlyingActor().getRepository().getRequestContextById(uuid);
                awaitCond(() -> ServerRequestContext.LogState.GLOBAL_ABORT == ctx.loggedState(),
                        Duration.create(ServerRequestContext.CONVERSATIONAL_TIMEOUT + 1, TimeUnit.SECONDS),
                        Duration.create(1, TimeUnit.SECONDS),
                        null);
            }
        };
    }

    @Test
    void testFinalDecisionTimeout() {
        new TestKit(system) {
            {
                // pass the probe as a server to server0
                var join = new JoinMessage(10, 19);
                server0.tell(join, testActor());

                var start = new StartMessage();
                server0.tell(start, ActorRef.noSender());

                var uuid = UUID.randomUUID();

                var readRequest = new ReadRequest(uuid, 0);
                server0.tell(readRequest, ActorRef.noSender());

                var voteRequest = new VoteRequest(uuid);
                server0.tell(voteRequest, ActorRef.noSender());

                // the probe, which, from the perspective of server0, is another server, should receive a decision request
                var decisionRequest = new DecisionRequest(uuid);
                expectMsg(Duration.create(ServerRequestContext.FINAL_DECISION_TIMEOUT_S + 1, TimeUnit.SECONDS),
                        decisionRequest);
            }
        };
    }
}
