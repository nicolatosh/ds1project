package it.unitn.arpino.ds1project.datastore;

class Lock {
    private final LockRepository lockRepository;
    public final int key;

    public Lock(LockRepository repository, int key) {
        this.lockRepository = repository;
        this.key = key;
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

    public boolean lock() {
        return lockRepository.lock(this);
    }

    public void unlock() {
        lockRepository.release(this);
    }
}
