package com.example;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class CSVExporter {
    public static void main(String[] args) {
        String csvFile = "people2.csv";
        String query = "SELECT * FROM person";

        try (Connection connection = DatabaseUtils.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query);
             BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(csvFile))) {

            // Write CSV header
            bufferedWriter.write("id,name,email,address,age");
            bufferedWriter.newLine();

            int rowCount = 0;
            // Write data rows
            while (resultSet.next()) {
                bufferedWriter.write(resultSet.getLong("id") + "," +
                        resultSet.getString("name") + "," +
                        resultSet.getString("email") + "," +
                        resultSet.getString("address") + "," +
                        resultSet.getInt("age"));
                bufferedWriter.newLine();

                rowCount++;

                // Flush periodically to avoid memory overload
                if (rowCount % 100_000 == 0) {
                    bufferedWriter.flush();
                    System.out.println(rowCount + " rows written...");
                }
            }

            bufferedWriter.flush(); // Final flush
            System.out.println("Data successfully exported to " + csvFile);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
