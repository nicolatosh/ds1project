package it.unitn.arpino.ds1project.datastore;

import java.util.ArrayList;
import java.util.List;

public class Transaction implements ITransaction {
    private final IWorkspace workspace;
    private final List<Lock> locks;

    public Transaction() {
        workspace = new Workspace();
        locks = new ArrayList<>();
    }

    @Override
    public IWorkspace getWorkspace() {
        return workspace;
    }

    @Override
    public List<Lock> getLocks() {
        return locks;
    }
}
