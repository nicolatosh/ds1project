package it.unitn.arpino.ds1project.nodes.context;

import java.util.UUID;

public class SimpleRequestContext extends RequestContext {
    private boolean decided;

    public SimpleRequestContext(UUID uuid) {
        super(uuid);
    }

    @Override
    public boolean isDecided() {
        return decided;
    }

    public void setDecided() {
        decided = true;
    }
}
