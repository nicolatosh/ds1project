package it.unitn.arpino.ds1project;

import akka.actor.AbstractActor;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.japi.pf.ReceiveBuilder;
import akka.testkit.TestKit;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MultipleActorsThreadingTest {

    @Test
    void actorsMayChangeThread() {
        var system = ActorSystem.create("test");

        new TestKit(system) {
            {
                var first = system.actorOf(Actor.props());
                var second = system.actorOf(Actor.props());

                // first test

                first.tell("threadId", testActor());
                long firstThreadBeforeTest = expectMsgClass(Long.class);
                second.tell("threadId", testActor());
                long secondThreadBeforeTest = expectMsgClass(Long.class);

                System.out.println("<<< First test >>>");
                System.out.println("testkit thread: " + Thread.currentThread().getId());
                System.out.println("thread of first actor: " + firstThreadBeforeTest);
                System.out.println("thread of second actor: " + secondThreadBeforeTest);

                Assertions.assertEquals(firstThreadBeforeTest, secondThreadBeforeTest);

                // second test

                second.tell(1, testActor());
                first.tell("threadId", testActor());
                long firstThreadAfterTest = expectMsgClass(Long.class);

                second.tell("threadId", testActor());
                long secondThreadAfterTest = expectMsgClass(Long.class);

                System.out.println("<<< Second test >>>");
                System.out.println("testkit thread: " + Thread.currentThread().getId());
                System.out.println("thread of first actor: " + firstThreadAfterTest);
                System.out.println("thread of second actor: " + secondThreadAfterTest);

                Assertions.assertNotEquals(firstThreadAfterTest, secondThreadAfterTest);
            }
        };
    }

    static class Actor extends AbstractActor {
        public static Props props() {
            return Props.create(Actor.class, Actor::new);
        }

        public static Props pinned() {
            return Props.create(Actor.class, Actor::new).withDispatcher("my-pinned-dispatcher");
        }

        @Override
        public Receive createReceive() {
            return new ReceiveBuilder()
                    .matchEquals("threadId", msg -> getSender().tell(Thread.currentThread().getId(), getSender()))
                    .match(Integer.class, i -> {
                        Thread.sleep(i * 1000);
                    })
                    .build();
        }
    }
}
