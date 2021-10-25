package it.unitn.arpino.ds1project.nodes.logger;

import akka.actor.AbstractActor;
import akka.event.Logging;
import akka.event.Logging.InitializeLogger;

import java.text.SimpleDateFormat;
import java.util.Date;

public class MyLogger extends AbstractActor {
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(
                        InitializeLogger.class,
                        msg -> getSender().tell(Logging.loggerInitialized(), getSelf()))
                .match(
                        Logging.LogEvent.class,
                        msg -> System.out.format("%s %s: %s\n", timeOf(msg), issuerOf(msg), msg.message()))
                .build();
    }

    private String timeOf(Logging.LogEvent event) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("HH:mm:ss:SSS");
        Date date = new Date(event.timestamp());
        return simpleDateFormat.format(date);
    }

    private String issuerOf(Logging.LogEvent event) {
        String nodeName = event.logSource();
        return nodeName.substring(nodeName.lastIndexOf("/") + 1);
    }
}
