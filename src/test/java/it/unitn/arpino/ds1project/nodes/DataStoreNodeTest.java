package it.unitn.arpino.ds1project.nodes;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.messages.StartMessage;
import it.unitn.arpino.ds1project.nodes.context.SimpleRequestContext;
import org.junit.jupiter.api.*;
import scala.concurrent.duration.Duration;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DataStoreNodeTest {
    ActorSystem system;
    TestActorRef<SimpleNode> node;

    @BeforeEach
    void setUp() {
        system = ActorSystem.create();
        node = TestActorRef.create(system, SimpleNode.props());
    }

    @AfterEach
    void tearDown() {
        TestKit.shutdownActorSystem(system, Duration.create(1, TimeUnit.SECONDS), true);
        node = null;
    }

    @Test
    @Order(1)
    void testStashAndStartMessage() {
        new TestKit(system) {
            {
                node.tell("Hello", testActor());
                // expectMsg("World"); // This fails, as the message is stashed until we don't send a StartMsg
                expectNoMessage();

                node.tell(new StartMessage(), ActorRef.noSender());
                expectMsg("World");
            }
        };
    }

    @Test
    @Order(2)
    void testNoDuplicateContexts() {
        SimpleRequestContext ctx = new SimpleRequestContext(UUID.randomUUID(), null);
        node.underlyingActor().getRepository().addRequestContext(ctx);
        node.underlyingActor().getRepository().addRequestContext(ctx);
        assertSame(1, node.underlyingActor().getRepository().getAllRequestContexts().size());
    }

    @Test
    @Order(3)
    void testGetContext() {
        SimpleRequestContext ctx = new SimpleRequestContext(UUID.randomUUID(), null);
        node.underlyingActor().getRepository().addRequestContext(ctx);
        assertEquals(1, node.underlyingActor().getRepository().getAllRequestContexts(Predicate.not(SimpleRequestContext::isDecided)).size());
        assertEquals(0, node.underlyingActor().getRepository().getAllRequestContexts(SimpleRequestContext::isDecided).size());
        ctx.setDecided();
        assertEquals(1, node.underlyingActor().getRepository().getAllRequestContexts(SimpleRequestContext::isDecided).size());
        assertEquals(0, node.underlyingActor().getRepository().getAllRequestContexts(Predicate.not(SimpleRequestContext::isDecided)).size());
    }

    @Test
    @Order(4)
    void crashAndResume() {
        new TestKit(system) {
            {
                node.tell(new StartMessage(), ActorRef.noSender());

                assertSame(DataStoreNode.Status.ALIVE, node.underlyingActor().getStatus());
                node.tell("Hello", testActor());
                expectMsg("World");

                node.underlyingActor().crash();
                assertSame(DataStoreNode.Status.CRASHED, node.underlyingActor().getStatus());
                node.tell("Hello", testActor());
                expectNoMessage();

                node.underlyingActor().resume();
                assertSame(DataStoreNode.Status.ALIVE, node.underlyingActor().getStatus());
                node.tell("Hello", testActor());
                expectMsg("World");
            }
        };
    }
}