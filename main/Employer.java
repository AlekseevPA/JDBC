package main;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Employer {

    public static final String TABLE = "employers";
    public static final String CREATE = "create table if not exists " + TABLE + " (\n" +
            "    id int(10) unsigned not null primary key auto_increment,\n" +
            "    first_name varchar(255) not null,\n" +
            "    last_name varchar(255) not null,\n" +
            "    middle_name varchar(255) not null,\n" +
            "    salary float(10) not null,\n" +
            "    position varchar(255) not null,\n" +
            "    login varchar(255) not null,\n" +
            "    password varchar(255) not null\n" +
            ")";
    public static final String INSERT = "INSERT INTO " + TABLE + " " +
            "(first_name, last_name, middle_name, salary, position, login, password) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)";
    public static final String SELECT = "SELECT " +
            "id, first_name, last_name, middle_name, salary, position, login, password " +
            "from " + TABLE;
    public static final String UPDATE = "UPDATE " + TABLE + " SET first_name = ?, last_name = ?," +
            "middle_name = ?, salary = ?, position = ?, login = ?,password = ?" +
            "WHERE id = ?";

    public static final String DELETE = "DELETE FROM " + TABLE + " WHERE id = ?";

    public static final String DROP_TABLE = "DROP TABLE IF EXISTS " + TABLE;

    public static final String DROP_FUNCTION_MAX_SALARY = "DROP FUNCTION IF EXISTS getEmployeeLastNameWithMaxSalary;";

    public static final String STORED_FUNCTION_MAX_SALARY =
            "CREATE FUNCTION getEmployeeLastNameWithMaxSalary() RETURNS VARCHAR(255)\n" +
            "BEGIN\n" +
            "\n" +
            "DECLARE result VARCHAR(255);\n" +
            "SELECT last_name into result FROM employers ORDER BY salary DESC LIMIT 1;\n" +
            "RETURN result;\n" +
            "\n" +
            "END;";

    private int id;
    private String firstName;
    private String lastName;
    private String middleName;
    private float salary;
    private String position;
    private String login;
    private String password;

    public Employer(String firstName, String lastName, String middleName, float salary, String position, String login, String password) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.middleName = middleName;
        this.position = position;
        this.salary = salary;
        this.login = login;
        this.password = password;
    }

    public static Employer fromCSVString(String line) {
        String[] parts = line.split(",");
        for (int i = 0; i < parts.length; i++) {
            parts[i] = parts[i].replace("\"", "");
        }

        return new Employer(
                parts[1],
                parts[0],
                parts[2],
                Float.parseFloat(parts[3]),
                parts[4],
                parts[5],
                Integer.toString(parts[6].hashCode())
        );

    }

    public static Employer fromResultSet(ResultSet resultSet) throws SQLException {
        Employer employer = new Employer(
                resultSet.getString(2),
                resultSet.getString(3),
                resultSet.getString(4),
                resultSet.getFloat(5),
                resultSet.getString(6),
                resultSet.getString(7),
                resultSet.getString(8)
        );

        employer.setId(resultSet.getInt(1));

        return employer;
    }

    public static List<Employer> select(Connection connection, String query) throws SQLException {
        List<Employer> employers = new ArrayList<>();
        try (Statement statement = connection.createStatement()) {
            try (ResultSet resultSet = statement.executeQuery(query)) {
                while (resultSet.next()) {
                    employers.add(Employer.fromResultSet(resultSet));
                }
            }
        }

        return employers;
    }

    public void insert(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS)) {

            fillStatement(statement);
            statement.executeUpdate();

            try (ResultSet resultSet = statement.getGeneratedKeys()) {
                if (resultSet.next()) {
                    id = resultSet.getInt(1);
                }
            }
        }
    }

    private void fillStatement(PreparedStatement statement) throws SQLException {
        statement.setString(1, firstName);
        statement.setString(2, lastName);
        statement.setString(3, middleName);
        statement.setFloat(4, salary);
        statement.setString(5, position);
        statement.setString(6, login);
        statement.setString(7, password);
    }

    public void update(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(UPDATE)) {
            fillStatement(statement);
            statement.setInt(8, id);

            statement.executeUpdate();
        }
    }

    public void delete(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(DELETE)) {
            statement.setInt(1, id);
            statement.executeUpdate();
        }
    }

    @Override
    public String toString() {
        return "Employer{" +
                "id=" + id +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", middleName='" + middleName + '\'' +
                ", salary=" + salary +
                ", position='" + position + '\'' +
                ", login='" + login + '\'' +
                ", password='" + password + '\'' +
                '}';
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public String getMiddleName() {
        return middleName;
    }

    public void setMiddleName(String middleName) {
        this.middleName = middleName;
    }

    public float getSalary() {
        return salary;
    }

    public void setSalary(float salary) {
        this.salary = salary;
    }

    public String getPosition() {
        return position;
    }

    public void setPosition(String position) {
        this.position = position;
    }

    public String getLogin() {
        return login;
    }

    public void setLogin(String login) {
        this.login = login;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
