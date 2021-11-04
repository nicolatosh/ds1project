package it.unitn.arpino.ds1project.datastore.lock;

import java.util.ArrayList;
import java.util.List;

public class LockManager implements ILockManager {
    private final List<Lock> locks;

    public LockManager() {
        locks = new ArrayList<>();
    }

    @Override
    public Lock getLock(int key) {
        return new Lock(this, key);
    }

    @Override
    public boolean lock(Lock lock) {
        if (locks.contains(lock)) {
            // The data item is already locked
            return false;
        }
        locks.add(lock);
        return true;
    }

    @Override
    public void release(Lock lock) {
        locks.remove(lock);
    }
}
