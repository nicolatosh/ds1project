package it.unitn.arpino.ds1project.datastore;

import java.util.Map;

public class Database implements IDatabase {
    Column versions;
    Column values;

    public Database() {
        this.versions = new Column();
        this.values = new Column();
    }

    @Override
    public void initializeDb(Map<Integer, Integer> initialValues) {

        // Setting up keys, values and versions. Versions by default are 0
        initialValues.forEach((k, v) -> {
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
}
