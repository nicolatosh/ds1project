package it.unitn.arpino.ds1project.datastore.lock;

public interface ILockManager {
    /**
     * Creates a lock for the data item with the specified key, which the caller has to be subsequently lock.
     *
     * @param key Key of the data item for which to return a lock.
     * @return A lock for the data item with the specified key.
     */
    Lock getLock(int key);

    /**
     * Attempts to lock the provided lock.
     *
     * @param lock The lock to attempt to lock.
     * @return Whether the lock has been locked or not. If false, then another transaction is already holding a locked
     * lock for the same data item.
     */
    boolean lock(Lock lock);

    /**
     * Releases the provided lock.
     *
     * @param lock The lock to release.
     */
    void release(Lock lock);
}
