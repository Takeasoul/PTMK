package org.example;

import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MyApp {
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/members";
    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "admin";
    private static final Random RANDOM = new Random();

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Please provide a mode of operation.");
            return;
        }

        int mode;
        try {
            mode = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.out.println("Invalid mode of operation.");
            return;
        }

        try (PostgresHandler dbHandler = new PostgresHandler(DB_URL, DB_USER, DB_PASSWORD)) {
            switch (mode) {
                case 0:
                    clearDatabase(dbHandler);
                    break;
                case 1:
                    createTable(dbHandler);
                    break;
                case 2:
                    if (args.length < 6) {
                        System.out.println("Please provide first name, last name, middle name, birth date, and gender.");
                        return;
                    }
                    String firstName = args[1];
                    String lastName = args[2];
                    String middleName = args[3];
                    LocalDate birthDate = LocalDate.parse(args[4], DateTimeFormatter.ISO_LOCAL_DATE);
                    String gender = args[5];
                    insertMemberDb(dbHandler, firstName, lastName, middleName, birthDate, gender);
                    break;
                case 3:
                    showAllMembersFiltred(dbHandler);
                    break;
                case 4:
                    generateMembers(dbHandler);
                    break;
                case 5:
                    findAndMeasure(dbHandler);
                    break;
                case 6:
                    optimizeAndMeasure(dbHandler);
                    break;
                default:
                    System.out.println("Invalid mode of operation.");
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void clearDatabase(PostgresHandler dbHandler) {
        try (Statement stmt = dbHandler.getConnection().createStatement()) {
            // Drop indexes
            stmt.execute("DROP INDEX IF EXISTS idx_gender_last_name");
            stmt.execute("DROP INDEX IF EXISTS idx_gender");
            stmt.execute("DROP INDEX IF EXISTS idx_last_name");

            // Truncate table and reset identity
            stmt.execute("TRUNCATE TABLE memberstable RESTART IDENTITY");
            System.out.println("Database cleared and indexes dropped successfully.");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void createTable(PostgresHandler dbHandler) {
        String sql = "CREATE TABLE IF NOT EXISTS memberstable (" +
                "id SERIAL PRIMARY KEY, " +
                "first_name VARCHAR(255), " +
                "last_name VARCHAR(255), " +
                "middle_name VARCHAR(255), " +
                "birth_date DATE, " +
                "gender VARCHAR(1))";
        try (Statement stmt = dbHandler.getConnection().createStatement()) {
            stmt.execute(sql);
            System.out.println("Table created successfully.");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void insertMemberDb(PostgresHandler dbHandler, String firstName, String lastName, String middleName, LocalDate birthDate, String gender) {
        MemberEntity member = new MemberEntity(firstName, lastName, middleName, birthDate, gender);
        try {
            member.insertMember(dbHandler);
            System.out.println("Members inserted successfully.");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }


    private static void showAllMembersFiltred(PostgresHandler dbHandler) {
        try {
            List<MemberEntity> memberList = dbHandler.getAllMembersFiltred();
            for (MemberEntity member : memberList) {
                System.out.printf("Full Name: %s, Birth Date: %s, Gender: %s, Age: %d%n",
                        member.getFullName(),
                        member.getBirthDate(),
                        member.getGender(),
                        member.calculateAge());
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }


    private static void generateMembers(PostgresHandler dbHandler) {
        List<MemberEntity> membersList = new ArrayList<>();
        String[] firstNames = {"Nikita", "Alina", "Alexey", "Anna", "Sasha"};
        String[] lastNames = {"Ivanov", "Petrov", "Fishkin", "Petrikov", "Filimonov"};
        String[] lastNamesWithF = {"Fishkin", "Filimonov"};
        String[] middleNames = {"Aleksandrovich", "Petrovich", "Artemich", "Sergeevich", "Ivanovich"};
         LocalDate[] DATES = {
                LocalDate.of(1980, 1, 1),
                LocalDate.of(1985, 6, 15),
                LocalDate.of(1990, 12, 25),
                LocalDate.of(1995, 3, 10),
                LocalDate.of(2000, 7, 4),
                LocalDate.of(2005, 8, 18),
                LocalDate.of(2010, 11, 30),
                LocalDate.of(2015, 2, 14),
                LocalDate.of(2020, 4, 1)
        };
        String[] genders = {"M", "F"};

        for (int i = 0; i < 1000000; i++) {
            String firstName = firstNames[RANDOM.nextInt(firstNames.length)];
            String lastName = lastNames[RANDOM.nextInt(lastNames.length)];
            String middleName = middleNames[RANDOM.nextInt(middleNames.length)];
            String gender = genders[RANDOM.nextInt(genders.length)];
            LocalDate birthDate = DATES[RANDOM.nextInt(DATES.length)];

            membersList.add(new MemberEntity(firstName, lastName, middleName, birthDate, gender));
        }

        for (int i = 0; i < 100; i++) {
            String firstName = firstNames[RANDOM.nextInt(firstNames.length)];
            String lastName = lastNames[RANDOM.nextInt(lastNamesWithF.length)];
            String middleName = middleNames[RANDOM.nextInt(middleNames.length)];
            LocalDate birthDate = DATES[RANDOM.nextInt(DATES.length)];

            membersList.add(new MemberEntity(firstName, lastName, middleName, birthDate, "M"));
        }

        try {
            dbHandler.insertMembersBatch(membersList);
            System.out.println("Members generated and inserted successfully.");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void findAndMeasure(PostgresHandler dbHandler) {
        long startTime = System.currentTimeMillis();
        List<MemberEntity> membersList;
        try {
            membersList = dbHandler.findMembersByGenderAndLastNamePrefix("M", "F");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return;
        }
        long endTime = System.currentTimeMillis();

        for (MemberEntity member : membersList) {
            System.out.printf("Full Name: %s, Birth Date: %s, Gender: %s, Age: %d%n",
                    member.getFullName(),
                    member.getBirthDate(),
                    member.getGender(),
                    member.calculateAge());
        }

        System.out.printf("Query executed in %d ms.%n", (endTime - startTime));
    }

    private static void optimizeAndMeasure(PostgresHandler dbHandler) {
        // Create indexes
        try {
            dbHandler.createIndex("CREATE INDEX idx_gender ON memberstable(gender)");
            dbHandler.createIndex("CREATE INDEX idx_last_name ON memberstable(last_name)");
            dbHandler.createIndex("CREATE INDEX idx_gender_last_name ON memberstable(gender, last_name)");
        } catch (SQLException e) {
            System.out.println("Error creating indexes: " + e.getMessage());
            return;
        }

        // Check index usage
        try {
            dbHandler.checkQueryPlan("SELECT first_name, last_name, middle_name, birth_date, gender FROM memberstable WHERE gender = 'M' AND last_name LIKE 'F%'");
        } catch (SQLException e) {
            System.out.println("Error checking query plan: " + e.getMessage());
            return;
        }

        // Measure time after creating indexes
        System.out.println("Indexes created. Measuring performance...");
        findAndMeasure(dbHandler);
    }



}
