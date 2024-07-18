package org.example;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.Period;

public class MemberEntity {
    private final String firstName;
    private final String lastName;
    private final String middleName;
    private final LocalDate birthDate;
    private final String gender;

    public MemberEntity(String firstName, String lastName, String middleName, LocalDate birthDate, String gender) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.middleName = middleName;
        this.birthDate = birthDate;
        this.gender = gender;
    }

    public int calculateAge() {
        return Period.between(this.birthDate, LocalDate.now()).getYears();
    }


    public String getFullName() {
        return String.format("%s %s %s", firstName, lastName, middleName);
    }

    public LocalDate getBirthDate() {
        return birthDate;
    }

    public String getGender() {
        return gender;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public String getMiddleName() {
        return middleName;
    }

    public void insertMember(PostgresHandler dbHandler) throws SQLException {
        String sql = "INSERT INTO memberstable (first_name, last_name, middle_name, birth_date, gender) " +
                "VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement pstmt = dbHandler.getConnection().prepareStatement(sql)) {
            pstmt.setString(1, this.getFirstName());
            pstmt.setString(2, this.getLastName());
            pstmt.setString(3, this.getMiddleName());
            pstmt.setDate(4, java.sql.Date.valueOf(this.getBirthDate()));
            pstmt.setString(5, this.getGender());
            pstmt.executeUpdate();
        }
    }
}