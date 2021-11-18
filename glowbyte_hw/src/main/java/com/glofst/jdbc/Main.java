package com.glofst.jdbc;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;

public class Main {
    private final String driverName = "org.h2.Driver";
    private final String login = "root";
    private final String password = "root";
    private final String url = "jdbc:h2:~/test";

    private String getElement(String[] array, String elem) {
        for (String iter: array) {
            if (iter.equalsIgnoreCase(elem)) {
                return iter;
            }
        }
        return null;
    }

    public void run() throws SQLException, IOException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            System.out.println("Can't get class. No driver found");
            e.printStackTrace();
            return;
        }
        Connection connection = DriverManager.getConnection(url, login, password);

        Statement statement = connection.createStatement();
        String sql = "SELECT * FROM TABLE_LIST;";
        ResultSet resultSet = statement.executeQuery(sql);
        HashMap<String, String[]> tableList = new HashMap<>();
        while(resultSet.next()) {
            String tableName = resultSet.getString("TABLE_NAME");
            String rawPK = resultSet.getString("PK");
            rawPK = rawPK.replaceAll("\\s+", "");
            String[] PK = rawPK.split(",");
            tableList.put(tableName, PK);
        }
        FileWriter fileWriter = new FileWriter("result.txt", false);
        sql = "SELECT * FROM TABLE_COLS;";
        resultSet = statement.executeQuery(sql);
        while(resultSet.next()) {
            String tableName = resultSet.getString("TABLE_NAME");
            String columnName = resultSet.getString("COLUMN_NAME");
            String columnType = resultSet.getString("COLUMN_TYPE");
            if (tableList.containsKey(tableName)) {
                String[] PK = tableList.get(tableName);
                String privateKey = getElement(PK, columnName);
                //if(Arrays.stream(PK).anyMatch(x -> x.equalsIgnoreCase(columnName))) {
                if (privateKey != null) {
                    String result = tableName + ", " + privateKey + ", " + columnType + "\n";
                    fileWriter.write(result);
                    fileWriter.flush();
                }
            }
        }

        fileWriter.close();

        connection.close();
    }

    public static void main(String[] args) {
        Main app = new Main();
        try {
            app.run();
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        }
    }
}
