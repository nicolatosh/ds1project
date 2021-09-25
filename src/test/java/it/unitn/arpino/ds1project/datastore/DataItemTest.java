package it.unitn.arpino.ds1project.datastore;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DataItemTest {

    @Test
    void compareTo() {
        DataItem first = new DataItem(5, 8, 10);
        DataItem second = new DataItem(5, 9, 10);
        assertEquals(-1, first.compareTo(second));
        assertEquals(1, second.compareTo(first));

        DataItem third = new DataItem(7, 8, 15);
        assertThrows(InvalidDataItemKeyException.class, () -> first.compareTo(third));
    }
}