package it.unitn.arpino.ds1project;

import akka.actor.AbstractActor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.testkit.TestActorRef;
import akka.testkit.TestKit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DispatcherThreadTest {
    @Test
    void twoActorsHaveSameThreads() {
        var system = ActorSystem.create();
        var first = system.actorOf(Actor.props());
        var second = system.actorOf(Actor.props());

        new TestKit(system) {
            {
                long testKitThread = Thread.currentThread().getId();

                first.tell("threadId", testActor());
                long firstActorThread = expectMsgClass(Long.class);

                second.tell("threadId", testActor());
                long secondActorThread = expectMsgClass(Long.class);

                System.out.println("testkit thread: " + testKitThread);
                System.out.println("first actor thread: " + firstActorThread);
                System.out.println("second actor thread: " + secondActorThread);

                Assertions.assertEquals(firstActorThread, secondActorThread);
            }
        };
    }

    @Test
    void twoActorsWithPinnedDispatchersHaveDifferentThreads() {
        var system = ActorSystem.create();
        var first = system.actorOf(Actor.pinned());
        var second = system.actorOf(Actor.pinned());

        new TestKit(system) {
            {
                long testKitThread = Thread.currentThread().getId();

                first.tell("threadId", testActor());
                long firstActorThread = expectMsgClass(Long.class);

                second.tell("threadId", testActor());
                long secondActorThread = expectMsgClass(Long.class);

                System.out.println("testkit thread: " + testKitThread);
                System.out.println("first actor thread: " + firstActorThread);
                System.out.println("second actor thread: " + secondActorThread);

                Assertions.assertNotEquals(firstActorThread, secondActorThread);
            }
        };
    }

    @Test
    void twoTestActorsHaveSameThreads() {
        var system = ActorSystem.create();
        TestActorRef<Actor> first = TestActorRef.create(system, Actor.props());
        TestActorRef<Actor> second = TestActorRef.create(system, Actor.props());

        new TestKit(system) {
            {
                long testKitThread = Thread.currentThread().getId();

                first.tell("threadId", testActor());
                long firstActorThread = expectMsgClass(Long.class);

                second.tell("threadId", testActor());
                long secondActorThread = expectMsgClass(Long.class);

                System.out.println("testkit thread: " + testKitThread);
                System.out.println("first actor thread: " + firstActorThread);
                System.out.println("second actor thread: " + secondActorThread);

                Assertions.assertEquals(firstActorThread, secondActorThread);
            }
        };
    }

    @Test
    void twoTestActorsWithPinnedDispatchersHaveDifferentThreads() {
        var system = ActorSystem.create();
        TestActorRef<Actor> first = TestActorRef.create(system, Actor.pinned());
        TestActorRef<Actor> second = TestActorRef.create(system, Actor.pinned());

        new TestKit(system) {
            {
                long testKitThread = Thread.currentThread().getId();

                first.tell("threadId", testActor());
                long firstActorThread = expectMsgClass(Long.class);

                second.tell("threadId", testActor());
                long secondActorThread = expectMsgClass(Long.class);

                System.out.println("testkit thread: " + testKitThread);
                System.out.println("first actor thread: " + firstActorThread);
                System.out.println("second actor thread: " + secondActorThread);

                Assertions.assertNotEquals(firstActorThread, secondActorThread);
            }
        };
    }

    static class Actor extends AbstractActor {
        static Props props() {
            return Props.create(Actor.class, Actor::new);
        }

        static Props pinned() {
            return Props.create(Actor.class, Actor::new).withDispatcher("my-pinned-dispatcher");
        }

        @Override
        public Receive createReceive() {
            return new ReceiveBuilder()
                    .matchEquals("threadId", msg -> getSender().tell(Thread.currentThread().getId(), getSender()))
                    .build();
        }
    }
}
