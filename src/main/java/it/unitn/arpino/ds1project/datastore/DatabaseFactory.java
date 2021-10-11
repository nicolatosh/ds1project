package it.unitn.arpino.ds1project.datastore;

public class DatabaseFactory {
    public IDatabaseController getController() {
        IDatabase database = new Database();
        return new DatabaseController(database);
    }
}
