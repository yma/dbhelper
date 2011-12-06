package fr.zenexity.dbhelper;

import java.sql.Connection;
import java.sql.DriverManager;

import org.junit.After;
import org.junit.Before;

public class TestingDatabase {

    protected Jdbc jdbc;

    public static class Entry {
        enum DistType { DEBIAN, UBUNTU, FEDORA, MANDRIVA, SLACKWARE }
        public String distName;
        public String version;
        public Double num;
        public DistType typeName;
        public DistType typeOrdinal;
    }

    public static class ExEntry extends Entry {
        public String fullName;
    }

    public static class EntryClob {
        public String textClob;
    }

    public static Connection getConnection(String driverClass, String connectionURL) {
        try {
            Class.forName(driverClass);
            return DriverManager.getConnection(connectionURL);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void loadData() {
        String jdbcDriver = System.getProperty("test.dbhelper.jdbcDriver");
        String jdbcURL = System.getProperty("test.dbhelper.jdbcURL");
        if (jdbcDriver == null || jdbcDriver.length() == 0 || jdbcDriver.charAt(0) == '$') {
            jdbcDriver = "org.h2.Driver";
        }
        if (jdbcURL == null || jdbcURL.length() == 0 || jdbcURL.charAt(0) == '$') {
            jdbcURL = "jdbc:h2:mem:";
        }
        jdbc = new Jdbc(getConnection(jdbcDriver, jdbcURL));

        boolean hsqldb = jdbcDriver.contains("hsqldb");
        String longText = hsqldb ? "LONGVARCHAR" : "LONGTEXT";

        JdbcStatement.executeUpdate(jdbc.connection, "DROP TABLE IF EXISTS Entry");
        JdbcStatement.executeUpdate(jdbc.connection, "CREATE TABLE Entry (distName VARCHAR(255) DEFAULT NULL, version VARCHAR(255), num FLOAT, typeName VARCHAR(255), typeOrdinal INT)");
        JdbcStatement insertEntry = jdbc.newStatement("INSERT INTO Entry (distName, version, num, typeName, typeOrdinal) VALUES (?, ?, ?, ?, ?)");
        try {
            insertEntry.executeUpdate("Debian", "5.0", 5, Entry.DistType.DEBIAN.name(), Entry.DistType.DEBIAN.ordinal());
            insertEntry.executeUpdate("Ubuntu", "9.10", 9.10, Entry.DistType.UBUNTU.name(), Entry.DistType.UBUNTU.ordinal());
            insertEntry.executeUpdate("Fedora", "12", 12, Entry.DistType.FEDORA.name(), Entry.DistType.FEDORA.ordinal());
            insertEntry.executeUpdate("Mandriva", "2010", 2010, Entry.DistType.MANDRIVA.name(), Entry.DistType.MANDRIVA.ordinal());
            insertEntry.executeUpdate("Slackware", "13.0", 13, Entry.DistType.SLACKWARE.name(), Entry.DistType.SLACKWARE.ordinal());
        } finally {
            insertEntry.close();
        }

        JdbcStatement.executeUpdate(jdbc.connection, "DROP TABLE IF EXISTS ExEntry");
        JdbcStatement.executeUpdate(jdbc.connection, "CREATE TABLE ExEntry (distName VARCHAR(255) DEFAULT NULL, version VARCHAR(255), num FLOAT, typeName VARCHAR(255), typeOrdinal INT, fullName VARCHAR(255))");
        JdbcStatement insertExEntry = jdbc.newStatement("INSERT INTO ExEntry (distName, version, num, typeName, typeOrdinal, fullName) VALUES (?, ?, ?, ?, ?, ?)");
        try {
            insertExEntry.executeUpdate("Debian", "5.0", 5, Entry.DistType.DEBIAN.name(), Entry.DistType.DEBIAN.ordinal(), "Debian 5.0");
            insertExEntry.executeUpdate("Ubuntu", "9.10", 9.10, Entry.DistType.UBUNTU.name(), Entry.DistType.UBUNTU.ordinal(), "Ubuntu 9.10");
            insertExEntry.executeUpdate("Fedora", "12", 12, Entry.DistType.FEDORA.name(), Entry.DistType.FEDORA.ordinal(), "Fedora 12");
            insertExEntry.executeUpdate("Mandriva", "2010", 2010, Entry.DistType.MANDRIVA.name(), Entry.DistType.MANDRIVA.ordinal(), "Mandriva 2010");
            insertExEntry.executeUpdate("Slackware", "13.0", 13, Entry.DistType.SLACKWARE.name(), Entry.DistType.SLACKWARE.ordinal(), "Slackware 13.0");
        } finally {
            insertExEntry.close();
        }

        JdbcStatement.executeUpdate(jdbc.connection, "DROP TABLE IF EXISTS EntryClob");
        JdbcStatement.executeUpdate(jdbc.connection, "CREATE TABLE EntryClob (textClob "+ longText +")");
        JdbcStatement insertEntryClob = jdbc.newStatement("INSERT INTO EntryClob (textClob) VALUES (?)");
        try {
            insertEntryClob.executeUpdate("Debian 5.0");
            insertEntryClob.executeUpdate("Ubuntu 9.10");
            insertEntryClob.executeUpdate("Fedora 12");
            insertEntryClob.executeUpdate("Mandriva 2010");
            insertEntryClob.executeUpdate("Slackware 13.0");
        } finally {
            insertExEntry.close();
        }
    }

    @After
    public void closeConnection() {
        jdbc.close();
    }

}
