package it.unitn.arpino.ds1project.datastore;

public class Connection implements IConnection {
    private final IDatabaseController controller;

    public Connection(IDatabaseController controller) {
        this.controller = controller;
    }

    @Override
    public int read(int key) {
        return controller.read(this, key);
    }

    @Override
    public void write(int key, int value) {
        controller.write(this, key, value);
    }

    @Override
    public IDatabaseController.Response prepare() {
        return controller.prepare(this);
    }

    @Override
    public void commit() {
        controller.commit(this);
    }

    @Override
    public void abort() {
        controller.abort(this);
    }
}
