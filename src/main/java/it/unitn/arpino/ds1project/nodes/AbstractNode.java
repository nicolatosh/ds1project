package it.unitn.arpino.ds1project.nodes;

import akka.actor.AbstractActorWithStash;
import it.unitn.arpino.ds1project.messages.TxnMessage;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * Base class for all nodes. Logs all received {@link TxnMessage}s.
 */
public abstract class AbstractNode extends AbstractActorWithStash {
    protected final Logger logger;

    public AbstractNode() {
        try (InputStream config = AbstractNode.class.getResourceAsStream("/logging.properties")) {
            if (config != null) {
                LogManager.getLogManager().readConfiguration(config);
            }
        } catch (IOException ignored) {
        }

        logger = Logger.getLogger(getSelf().path().name());
    }
}
