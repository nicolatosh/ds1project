package it.unitn.arpino.ds1project.datastore.lock;

public class Lock {
    private final ILockManager repository;
    public final int key;

    public Lock(ILockManager repository, int key) {
        this.repository = repository;
        this.key = key;
    }

    public final boolean lock() {
        return repository.lock(this);
    }

    public final void unlock() {
        repository.release(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Lock lock = (Lock) o;
        return key == lock.key;
    }

    @Override
    public int hashCode() {
        return key;
    }
}
