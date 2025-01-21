package com.example;

import com.github.javafaker.Faker;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Main {
    private static final int TOTAL_RECORDS = 10_000_000;
    private static final int THREAD_COUNT = 10;
    private static final int RECORDS_PER_THREAD = TOTAL_RECORDS / THREAD_COUNT;

    public static void main(String[] args) {
        Thread[] threads = new Thread[THREAD_COUNT];

        for (int i = 0; i < THREAD_COUNT; i++) {
            final int startId = i * RECORDS_PER_THREAD;
            threads[i] = new Thread(new DataInserter(startId, RECORDS_PER_THREAD));
            threads[i].start();
        }

        // Wait for all threads to finish
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Data insertion complete!");
    }
}


class DataInserter implements Runnable {
    private static final String DB_URL = "jdbc:mariadb://localhost:3306/heavy_load";
    private static final String DB_USER = "root";
    private static final String DB_PASS = ""; // Empty password

    private final int start;
    private final int count;
    private final Faker faker = new Faker();

    public DataInserter(int start, int count) {
        this.start = start;
        this.count = count;
    }

    @Override
    public void run() {
        String insertSQL = "INSERT INTO person (name, email, address, age) VALUES (?, ?, ?, ?)";

        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
             PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {

            connection.setAutoCommit(false);

            for (int i = 0; i < count; i++) {
                preparedStatement.setString(1, faker.name().fullName());
                preparedStatement.setString(2, faker.internet().emailAddress());
                preparedStatement.setString(3, faker.address().fullAddress());
                preparedStatement.setInt(4, faker.number().numberBetween(18, 100));

                preparedStatement.addBatch();

                if (i % 10_000 == 0) { // Commit in batches of 10,000
                    preparedStatement.executeBatch();
                    connection.commit();
                    System.out.println(Thread.currentThread().getName() + " inserted " + (start + i) + " records");
                }
            }

            preparedStatement.executeBatch();
            connection.commit();
            System.out.println(Thread.currentThread().getName() + " completed insertion.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
