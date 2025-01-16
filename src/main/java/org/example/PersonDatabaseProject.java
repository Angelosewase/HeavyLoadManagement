package org.example;

import com.github.javafaker.Faker;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.HashSet;
import java.util.Set;

public class PersonDatabaseProject {
    private static final String DB_URL = "jdbc:mysql://localhost:3306/Person";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "";

    public static void main(String[] args) {
        try {
            generateAndInsertPersons(1_000_000); // Adjust number for testing, use 10_000_000 for production
            exportToCSV("people.csv");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void generateAndInsertPersons(int numberOfRecords) throws SQLException {
        Faker faker = new Faker();
        Set<String> uniqueEmails = new HashSet<>();

        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
            String insertSQL = "INSERT INTO Person (name, email, address, age) VALUES (?, ?, ?, ?)";
            connection.setAutoCommit(false); // Enable batch processing

            try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {
                for (int i = 1; i <= numberOfRecords; i++) {
                    String email;
                    do {
                        email = faker.internet().emailAddress();
                    } while (!uniqueEmails.add(email)); // Ensure unique email

                    String name = faker.name().fullName();
                    String address = faker.address().fullAddress();
                    int age = faker.number().numberBetween(18, 99);

                    preparedStatement.setString(1, name);
                    preparedStatement.setString(2, email);
                    preparedStatement.setString(3, address);
                    preparedStatement.setInt(4, age);

                    preparedStatement.addBatch();

                    if (i % 10_000 == 0) { // Execute batch every 10,000 records
                        preparedStatement.executeBatch();
                        connection.commit();
                        System.out.println(i + " records inserted...");
                    }
                }
                preparedStatement.executeBatch(); // Insert remaining records
                connection.commit();
            }
        }
        System.out.println("Data inserted successfully.");
    }

    private static void exportToCSV(String fileName) throws SQLException, IOException {
        String query = "SELECT * FROM Person";

        try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query);
             FileWriter csvWriter = new FileWriter(fileName)) {

            // Write CSV header
            csvWriter.append("ID,Name,Email,Address,Age\n");

            // Write data rows
            while (resultSet.next()) {
                csvWriter.append(resultSet.getInt("id") + ",");
                csvWriter.append(resultSet.getString("name") + ",");
                csvWriter.append(resultSet.getString("email") + ",");
                csvWriter.append(resultSet.getString("address") + ",");
                csvWriter.append(resultSet.getInt("age") + "\n");
            }
        }
        System.out.println("Data exported to CSV successfully.");
    }
}
