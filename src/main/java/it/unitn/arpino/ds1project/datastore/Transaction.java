package it.unitn.arpino.ds1project.datastore;

import java.util.ArrayList;
import java.util.List;

public class Transaction implements ITransaction {
    private final ILockRepository lockRepository;
    private final IWorkspace workspace;
    private final List<Lock> locks;

    public Transaction(ILockRepository lockRepository) {
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
