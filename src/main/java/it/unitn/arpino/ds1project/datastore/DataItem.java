package it.unitn.arpino.ds1project.datastore;

import java.io.Serializable;

public class DataItem implements Comparable<DataItem>, Serializable {
    private final int key;
    private final int version;
    private final int value;

    public DataItem(int key, int version, int value) {
        this.key = key;
        this.version = version;
        this.value = value;
    }

    public int getKey() {
        return key;
    }

    public int getVersion() {
        return version;
    }

    public int getValue() {
        return value;
    }

    @Override
    public int compareTo(DataItem dataItem) throws InvalidDataItemKeyException {
        if (this.key != dataItem.key) {
            throw new InvalidDataItemKeyException("Attempted to compare DataItems with different keys: " +
                    this.key + " and " + dataItem.key);
        }

        int comparison = 0;
        if (this.version < dataItem.version) {
            comparison = -1;
        }
        if (this.version > dataItem.version) {
            comparison = 1;
        }
        return comparison;
    }

    @Override
    public int hashCode() {
        return this.key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataItem dataItem = (DataItem) o;
        return key == dataItem.key && version == dataItem.version && value == dataItem.value;
    }
}
