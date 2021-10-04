package it.unitn.arpino.ds1project.datastore;

import java.util.HashSet;
import java.util.Set;

public class LockRepository {
    private final Set<Lock> locks;

    public LockRepository() {
        locks = new HashSet<>();
    }

    public Lock getLock(int key) {
        return new Lock(this, key);
    }

    public boolean lock(Lock lock) {
        if (locks.contains(lock)) {
            // The data item is already locked
            return false;
        }
        locks.add(lock);
        return true;
    }

    public void release(Lock lock) {
        locks.remove(lock);
    }
}
