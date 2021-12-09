package it.unitn.arpino.ds1project.nodes;

import akka.actor.ActorSystem;
import akka.testkit.TestKit;
import org.junit.jupiter.api.Test;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

public class ConditionPoll {
    private static Boolean bool;

    @Test
    void name() {
        var system = ActorSystem.create("test");
        new TestKit(system) {
            {
                System.out.println("start");
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Thread.sleep(1000);
                            System.out.println("setting to true");
                            bool = true;
                        } catch (InterruptedException ignored) {
                        }
                    }
                }.run();
                awaitCond(() -> {
                            return bool;
                        },
                        Duration.create(4, TimeUnit.SECONDS),
                        Duration.create(1, TimeUnit.SECONDS),
                        "message");
            }
        };
        system.terminate();
    }
}
