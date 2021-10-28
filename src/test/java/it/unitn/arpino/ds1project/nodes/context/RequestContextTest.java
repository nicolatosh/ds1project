package it.unitn.arpino.ds1project.nodes.context;

import it.unitn.arpino.ds1project.messages.Message;
import it.unitn.arpino.ds1project.messages.Transactional;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RequestContextTest {

    @Test
    void test() {
        ContextManager<SimpleContext> contextManager = new ContextManager<>();

        SimpleMessage simpleMessage = new SimpleMessage(UUID.randomUUID());
        SimpleContext context = new SimpleContext(simpleMessage.uuid);

        contextManager.setActive(context);
        assertTrue(contextManager.contextOf(simpleMessage).isPresent());
        assertEquals(RequestContext.Status.ACTIVE, context.status);

        contextManager.setCompleted(context);
        assertTrue(contextManager.contextOf(simpleMessage).isPresent());
        assertEquals(RequestContext.Status.COMPLETED, context.status);
    }

    private static class SimpleContext extends RequestContext {
        public SimpleContext(UUID uuid) {
            super(uuid);
        }
    }

    private static class SimpleMessage extends Message implements Transactional {
        UUID uuid;

        public SimpleMessage(UUID uuid) {
            this.uuid = uuid;
        }

        @Override
        public TYPE getType() {
            return TYPE.Conversational;
        }

        @Override
        public UUID uuid() {
            return uuid;
        }
    }
}