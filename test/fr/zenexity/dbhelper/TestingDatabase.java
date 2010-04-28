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

    public static Connection initdb_HSQLMEM() {
        try {
            Class.forName("org.hsqldb.jdbcDriver");
            Connection connection = DriverManager.getConnection("jdbc:hsqldb:mem:dbhelper", "sa", "");
            JdbcStatement.executeUpdate(connection, "DROP TABLE IF EXISTS Entry");
            JdbcStatement.executeUpdate(connection, "CREATE TABLE Entry (distName VARCHAR(255) DEFAULT NULL, version VARCHAR(255), num FLOAT, typeName VARCHAR(255), typeOrdinal INT)");
            return connection;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void loadData() {
        jdbc = new Jdbc(initdb_HSQLMEM());
        JdbcStatement insertEntry = jdbc.newStatement("INSERT INTO Entry (distName, version, num, typeName, typeOrdinal) VALUES (?, ?, ?, ?, ?)");
        try {
            insertEntry.executeUpdate("Debian", "5.0", 5, Entry.DistType.DEBIAN, Entry.DistType.DEBIAN.ordinal());
            insertEntry.executeUpdate("Ubuntu", "9.10", 9.10, Entry.DistType.UBUNTU, Entry.DistType.UBUNTU.ordinal());
            insertEntry.executeUpdate("Fedora", "12", 12, Entry.DistType.FEDORA, Entry.DistType.FEDORA.ordinal());
            insertEntry.executeUpdate("Mandriva", "2010", 2010, Entry.DistType.MANDRIVA, Entry.DistType.MANDRIVA.ordinal());
            insertEntry.executeUpdate("Slackware", "13.0", 13, Entry.DistType.SLACKWARE, Entry.DistType.SLACKWARE.ordinal());
        } finally {
            insertEntry.close();
        }
    }

    @After
    public void closeConnection() {
        jdbc.close();
    }

}
