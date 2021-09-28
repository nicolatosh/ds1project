package it.unitn.arpino.ds1project.nodes;

import it.unitn.arpino.ds1project.transaction.Txn;


public abstract class AbstractViewManager<T extends AbstractNode> {
    protected T node;

    public AbstractViewManager(T node) {
        this.node = node;
    }

    protected abstract void changeView(Txn txn);

    protected abstract void sync();
}

