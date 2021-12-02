package it.unitn.arpino.ds1project.datastore.database;

import it.unitn.arpino.ds1project.datastore.workspace.IWorkspace;

import java.util.Map;

public class Database implements IDatabase {
    protected Column versions;
    protected Column values;

    public Database() {
        this.versions = new Column();
        this.values = new Column();
    }

    public Database(Map<Integer, Integer> keyValues) {
        this();
        keyValues.forEach((k, v) -> {
            values.put(k, v);
            versions.put(k, 0);
        });
    }

    @Override
    public Integer read(int key) {
        return values.get(key);
    }

    @Override
    public void write(int key, int value) {
        values.put(key, value);
    }

    @Override
    public Integer getVersion(int key) {
        return versions.get(key);
    }

    @Override
    public void setVersion(int key, int version) {
        versions.put(key, version);
    }

    @Override
    public boolean validate(IWorkspace workspace) {
        return workspace.getModifiedKeys().stream()
                .allMatch(key -> workspace.getVersion(key).equals(getVersion(key)));
    }

    @Override
    public IDatabase copy() {
        IDatabase database = new Database();
        versions.forEach(database::setVersion);
        values.forEach(database::write);

        return database;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Database database = (Database) o;
        return versions.equals(database.versions) && values.equals(database.values);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        return "Database{" +
                "versions=" + versions +
                ", values=" + values +
                '}';
    }
}
