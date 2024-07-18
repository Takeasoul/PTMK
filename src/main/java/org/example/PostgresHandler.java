package org.example;

import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class PostgresHandler implements AutoCloseable {
    private final Connection connection;

    public PostgresHandler(String url, String user, String password) throws SQLException {
        this.connection = DriverManager.getConnection(url, user, password);
    }


    public List<MemberEntity> getAllMembersFiltred() throws SQLException {
        List<MemberEntity> members = new ArrayList<>();
        String sql = "SELECT DISTINCT ON (first_name, last_name, middle_name, birth_date) " +
                "first_name, last_name, middle_name, birth_date, gender " +
                "FROM membersTable " +
                "ORDER BY first_name, last_name, middle_name, birth_date;";

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {

            while (rs.next()) {
                String firstName = rs.getString("first_name");
                String lastName = rs.getString("last_name");
                String middleName = rs.getString("middle_name");
                LocalDate birthDate = rs.getDate("birth_date").toLocalDate();
                String gender = rs.getString("gender");

                MemberEntity memberEntity = new MemberEntity(firstName, lastName, middleName, birthDate, gender);
                members.add(memberEntity);
            }
        }
        return members;
    }

    public void insertMembersBatch(List<MemberEntity> memberEntityList) throws SQLException {
        String sql = "INSERT INTO membersTable (first_name, last_name, middle_name, birth_date, gender) VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            for (MemberEntity memberEntity : memberEntityList) {
                pstmt.setString(1, memberEntity.getFirstName());
                pstmt.setString(2, memberEntity.getLastName());
                pstmt.setString(3, memberEntity.getMiddleName());
                pstmt.setDate(4, java.sql.Date.valueOf(memberEntity.getBirthDate()));
                pstmt.setString(5, memberEntity.getGender());
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }

    public List<MemberEntity> findMembersByGenderAndLastNamePrefix(String gender, String lastNamePrefix) throws SQLException {
        String sql = "SELECT first_name, last_name, middle_name, birth_date, gender FROM membersTable WHERE gender = ? AND last_name LIKE ?";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            pstmt.setString(1, gender);
            pstmt.setString(2, lastNamePrefix + "%");
            ResultSet rs = pstmt.executeQuery();

            List<MemberEntity> membersList = new ArrayList<>();
            while (rs.next()) {
                String firstName = rs.getString("first_name");
                String lastName = rs.getString("last_name");
                String middleName = rs.getString("middle_name");
                LocalDate birthDate = rs.getDate("birth_date").toLocalDate();
                String genderResult = rs.getString("gender");

                membersList.add(new MemberEntity(firstName, lastName, middleName, birthDate, genderResult));
            }
            return membersList;
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public void createIndex(String sql) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    public void checkQueryPlan(String query) throws SQLException {
        String sql = "EXPLAIN ANALYZE " + query;
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
        }
    }

    @Override
    public void close() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

}