package it.unitn.arpino.ds1project.datastore.lock;

public class Lock {
    private final ILockRepository repository;
    public final int key;

    public Lock(ILockRepository repository, int key) {
        this.repository = repository;
        this.key = key;
    }

    public boolean lock() {
        return repository.lock(this);
    }

    public void unlock() {
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
