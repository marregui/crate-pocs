package io.crate.pocs.cli;

import java.sql.*;
import java.util.Properties;

public class PoisonPills {

    private static final Properties PROPS = new Properties();
    static {
        PROPS.put("user", "crate");
        PROPS.put("password", "");
    }

    public static void main(String[] args) throws Exception {

        String select = "SELECT date_format(ended), error, stmt " +
                "FROM sys.jobs_log " +
                "WHERE error IS NOT NULL and stmt != 'ROLLBACK'";
        try (Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost:5433/", PROPS)) {
            try (Statement stmt = conn.createStatement()) {
                if (stmt.execute(select)) {
                    ResultSet rs = stmt.getResultSet();
                    while (rs.next()) {
                        String end = rs.getString(1);
                        String error = rs.getString(2);
                        String query = rs.getString(3);
                        System.out.printf("%s ERROR: %s\n  - query: %s\n", end, error, query);
                    }
                }
            }
        }
    }
}