package it.unitn.arpino.ds1project.nodes;

import akka.actor.AbstractActor;
import it.unitn.arpino.ds1project.messages.Message;
import scala.PartialFunction;
import scala.runtime.BoxedUnit;

import java.util.logging.Logger;

public abstract class AbstractNode extends AbstractActor {
    protected final Logger logger;

    public AbstractNode() {
        logger = Logger.getLogger(getSelf().path().name());
    }

    @Override
    public void aroundReceive(PartialFunction<Object, BoxedUnit> receive, Object msg) {
        if (msg instanceof Message) {
            Message message = (Message) msg;

            logger.info("received " + message.getType() +
                    "/" + message.getClass().getSimpleName() +
                    " from " + getSender().path().name());
        }

        super.aroundReceive(receive, msg);
    }
}
