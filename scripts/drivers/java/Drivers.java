// Driver-matrix scenario with MySQL Connector/J and server-side prepared statements.
import java.math.BigDecimal;
import java.sql.*;
import java.util.Arrays;
import java.util.Objects;

public class Drivers {
    static String env(String k, String def) {
        String v = System.getenv(k);
        return v == null || v.isEmpty() ? def : v;
    }

    static void expect(String what, Object got, Object want) {
        if (!Objects.equals(got, want)) {
            System.out.println("java: FAIL " + what + ": got " + got + ", want " + want);
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception {
        String url = "jdbc:mysql://" + env("DRV_HOST", "127.0.0.1") + ":" + env("DRV_PORT", "3306") + "/"
            + "?useServerPrepStmts=true&sslMode=DISABLED&allowPublicKeyRetrieval=true&characterEncoding=UTF-8&connectionTimeZone=UTC";
        try (Connection c = DriverManager.getConnection(url, env("DRV_USER", "root"), env("DRV_PASSWORD", ""))) {
            try (Statement s = c.createStatement()) {
                s.execute("CREATE DATABASE IF NOT EXISTS " + env("DRV_DB", "app"));
            }
            c.setCatalog(env("DRV_DB", "app"));
            try (Statement s = c.createStatement()) {
                s.execute("DROP TABLE IF EXISTS drv_java");
                s.execute("CREATE TABLE drv_java (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(50) NOT NULL,"
                    + " amount DECIMAL(10,2), created DATETIME, data BLOB, flag BOOL, UNIQUE KEY un (name))");
            }
            Timestamp when = Timestamp.valueOf("2026-01-02 03:04:05");
            try (PreparedStatement p = c.prepareStatement(
                    "INSERT INTO drv_java (name, amount, created, data, flag) VALUES (?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS)) {
                p.setString(1, "héllo 🌍");
                p.setBigDecimal(2, new BigDecimal("12.34"));
                p.setTimestamp(3, when);
                p.setBytes(4, new byte[] {0, 1, 2, (byte) 255});
                p.setBoolean(5, true);
                expect("executeUpdate", p.executeUpdate(), 1);
                try (ResultSet k = p.getGeneratedKeys()) {
                    k.next();
                    expect("generated key", k.getLong(1), 1L);
                }
            }
            try (PreparedStatement p = c.prepareStatement(
                    "INSERT INTO drv_java (name, amount, created, data, flag) VALUES (?, NULL, NULL, NULL, NULL), (?, ?, ?, ?, ?)")) {
                p.setString(1, "nulls");
                p.setString(2, "second");
                p.setBigDecimal(3, new BigDecimal("-0.50"));
                p.setTimestamp(4, when);
                p.setBytes(5, "x".getBytes());
                p.setBoolean(6, false);
                expect("rows affected", p.executeUpdate(), 2);
            }
            try (PreparedStatement p = c.prepareStatement("SELECT name, amount, created, data, flag FROM drv_java WHERE id = ?")) {
                p.setInt(1, 1);
                try (ResultSet r = p.executeQuery()) {
                    r.next();
                    expect("utf8mb4 string", r.getString(1), "héllo 🌍");
                    expect("decimal", r.getBigDecimal(2), new BigDecimal("12.34"));
                    expect("datetime", r.getTimestamp(3), when);
                    expect("blob", Arrays.toString(r.getBytes(4)), Arrays.toString(new byte[] {0, 1, 2, (byte) 255}));
                    expect("bool", r.getBoolean(5), true);
                }
            }
            try (PreparedStatement p = c.prepareStatement("SELECT amount, created, data, flag FROM drv_java WHERE name = ?")) {
                p.setString(1, "nulls");
                try (ResultSet r = p.executeQuery()) {
                    r.next();
                    expect("NULLs", Arrays.asList(r.getObject(1), r.getObject(2), r.getObject(3), r.getObject(4)),
                        Arrays.asList(null, null, null, null));
                }
            }
            c.setAutoCommit(false);
            try (Statement s = c.createStatement()) {
                s.executeUpdate("UPDATE drv_java SET amount = amount + 1 WHERE id = 1");
                c.rollback();
                s.executeUpdate("UPDATE drv_java SET amount = amount + 1 WHERE id = 1");
                c.commit();
            }
            c.setAutoCommit(true);
            try (Statement s = c.createStatement(); ResultSet r = s.executeQuery("SELECT amount FROM drv_java WHERE id = 1")) {
                r.next();
                expect("commit/rollback", r.getBigDecimal(1), new BigDecimal("13.34"));
            }
            try (PreparedStatement p = c.prepareStatement("INSERT INTO drv_java (name) VALUES (?)")) {
                p.setString(1, "second");
                p.executeUpdate();
                expect("duplicate key error", "no error", 1062);
            } catch (SQLException e) {
                expect("duplicate key error", e.getErrorCode(), 1062);
            }
        }
        System.out.println("java: PASS");
    }
}
