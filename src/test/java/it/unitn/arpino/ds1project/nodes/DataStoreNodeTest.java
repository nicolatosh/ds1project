package it.unitn.arpino.ds1project.nodes;

import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import it.unitn.arpino.ds1project.nodes.context.RequestContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

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
        TestKit.shutdownActorSystem(system, scala.concurrent.duration.Duration.create(1, TimeUnit.SECONDS), false);
        node = null;
    }

    @Test
    @Order(1)
    void testNoDuplicateContexts() {
        SimpleRequestContext ctx = new SimpleRequestContext(UUID.randomUUID());
        node.underlyingActor().addContext(ctx);
        node.underlyingActor().addContext(ctx);
        assertSame(1, node.underlyingActor().getActive().size());
    }

    @Test
    @Order(2)
    void testGetContext() {
        SimpleRequestContext ctx = new SimpleRequestContext(UUID.randomUUID());
        node.underlyingActor().addContext(ctx);
        assertEquals(1, node.underlyingActor().getActive().size());
        assertEquals(0, node.underlyingActor().getDecided().size());
        ctx.decided = true;
        assertEquals(0, node.underlyingActor().getActive().size());
        assertEquals(1, node.underlyingActor().getDecided().size());
    }

    @Test
    @Order(3)
    void crashAndResume() {
        new TestKit(system) {
            {
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


    private static class SimpleNode extends DataStoreNode<SimpleRequestContext> {
        public static Props props() {
            return Props.create(SimpleNode.class, SimpleNode::new);
        }

        @Override
        public Receive createReceive() {
            return ReceiveBuilder.create()
                    .matchEquals("Hello", msg -> getSender().tell("World", getSelf()))
                    .build();
        }
    }

    private static class SimpleRequestContext extends RequestContext {
        private boolean decided;

        public SimpleRequestContext(UUID uuid) {
            super(uuid);
        }

        @Override
        public boolean isDecided() {
            return decided;
        }
    }
}