package it.unitn.arpino.ds1project.datastore;

public interface ILockRepository {
    Lock getLock(int key);

    boolean lock(Lock lock);

    void release(Lock lock);
}
