package it.unitn.arpino.ds1project.datastore.transaction;

import it.unitn.arpino.ds1project.datastore.lock.ILockManager;
import it.unitn.arpino.ds1project.datastore.lock.Lock;
import it.unitn.arpino.ds1project.datastore.workspace.IWorkspace;
import it.unitn.arpino.ds1project.datastore.workspace.Workspace;

import java.util.ArrayList;
import java.util.List;

public class Transaction implements ITransaction {
    private final ILockManager lockRepository;
    private final IWorkspace workspace;
    private final List<Lock> locks;

    public Transaction(ILockManager lockRepository) {
        this.lockRepository = lockRepository;
        workspace = new Workspace();
        locks = new ArrayList<>();
    }

    @Override
    public IWorkspace getWorkspace() {
        return workspace;
    }

    @Override
    public boolean acquireLocks() {
        workspace.getModifiedKeys().stream()
                .map(lockRepository::getLock)
                .forEach(locks::add);

        if (locks.stream().allMatch(Lock::lock)) {
            return true;
        }

        releaseLocks();
        return false;
    }

    @Override
    public void releaseLocks() {
        locks.forEach(Lock::unlock);
    }
}
