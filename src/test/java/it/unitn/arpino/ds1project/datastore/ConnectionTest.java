package it.unitn.arpino.ds1project.datastore;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

class ConnectionTest {
    IDatabase database;
    IDatabaseController controller;

    private IDatabase preloadedDatabase() {
        IDatabase database = new Database();
        IntStream.range(0, 10).forEach(i -> {
            database.write(i, 100);
            database.setVersion(i, 1);
        });
        return database;
    }

    @BeforeEach
    void setUp() {
        database = preloadedDatabase();
        controller = new DatabaseController(database);

        // Preload the database with some data
        IntStream.range(0, 10).forEach(i -> {
            database.write(i, 100);
            database.setVersion(i, 1);
        });
    }

    @Test
    void read() {
        IConnection connection = controller.beginTransaction();

        IntStream.range(0, 10).forEach(i -> Assertions.assertEquals(100, connection.read(i)));
    }

    @Test
    void readYourWrites() {
        IConnection connection = controller.beginTransaction();

        connection.write(4, 40);
        connection.write(7, 70);

        Assertions.assertEquals(40, connection.read(4));
        Assertions.assertEquals(70, connection.read(7));
    }

    @Test
    void prepare_NonInterferingTransactions() {
        IConnection first = controller.beginTransaction();
        IConnection second = controller.beginTransaction();

        // Concurrent interleaving, reading different keys
        first.write(4, 40);
        second.write(8, 80);
        first.write(7, 70);
        second.write(3, 30);

        // both transactions should be preparable
        Assertions.assertTrue(first.prepare());
        Assertions.assertTrue(second.prepare());
    }

    @Test
    void prepare_InterferingTransactions() {
        IConnection first = controller.beginTransaction();
        IConnection second = controller.beginTransaction();

        // Common keys are: 5, 6

        first.write(4, 40);
        first.write(5, 50);
        first.write(6, 60);
        first.write(7, 70);

        second.write(3, 30);
        second.write(5, 500);
        second.write(6, 600);
        second.write(8, 80);

        // only one should be preparable
        Assertions.assertTrue(first.prepare());
        Assertions.assertFalse(second.prepare());
    }

    @Test
    void commit() {
        IConnection connection = controller.beginTransaction();

        connection.write(4, 40);
        connection.write(7, 70);

        connection.prepare();
        connection.commit();


        IDatabase expected = preloadedDatabase();
        expected.write(4, 40);
        expected.setVersion(4, 2);
        expected.write(7, 70);
        expected.setVersion(7, 2);

        Assertions.assertEquals(expected, database);
    }

    @Test
    void abort() {
        IConnection connection = controller.beginTransaction();

        connection.write(4, 40);
        connection.write(7, 70);

        connection.prepare();
        connection.abort();

        // database remains unmodified
        IDatabase expected = preloadedDatabase();
        Assertions.assertEquals(expected, database);
    }
}