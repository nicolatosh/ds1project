package it.unitn.arpino.ds1project.nodes.context;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

class RequestContextTest {
    @Test
    void testEquals() {
        RequestContext ctx1 = new RequestContext(UUID.randomUUID(), null) {
            @Override
            public boolean isDecided() {
                return false;
            }
        };

        RequestContext ctx2 = new RequestContext(UUID.randomUUID(), null) {
            @Override
            public boolean isDecided() {
                return false;
            }
        };

        assertNotEquals(ctx1, ctx2);
    }
}