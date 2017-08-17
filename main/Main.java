package main;

import db.MySQLConnection;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Main {

    public static void main(String[] args) throws IOException {
        Properties dbProperties = loadProperties();

        try {
            MySQLConnection.checkDriverExistence();
        } catch (ClassNotFoundException e) {
            System.err.println("Нет соответствующего драйвера.");
            System.err.println(e.getMessage());
        }

        try (Connection connection = new MySQLConnection(dbProperties).getConnection()) {
            createEmployersTable(connection);
            createStoredFunction(connection);

            List<Employer> employers = parseEmployersCSV(
                    Main.class.getClassLoader().getResourceAsStream("employers.csv")
            );

            System.out.println("\nЗапрос для добавление записи:");
            System.out.println(Employer.INSERT);
            System.out.println("\nEmployers:");
            for (Employer employer : employers) {
                employer.insert(connection);
            }

            String query = Employer.SELECT + " order by id desc";
            System.out.println("\nЗапрос для выборки: ");
            System.out.println(query);

            employers = Employer.select(connection, query);
            for (Employer employer : employers) {
                System.out.println(employer);
            }

            System.out.println("\nЗапрос для обновления: ");
            System.out.println(Employer.UPDATE);

            Employer firstEmployee = employers.get(5);
            Employer secondEmployee = employers.get(7);
            firstEmployee.setFirstName("Измененное имя 1");
            secondEmployee.setFirstName("Измененное имя 2");
            firstEmployee.update(connection);
            secondEmployee.update(connection);

            System.out.println("\nОбновленные записи: ");
            employers = Employer.select(connection, query);
            for (Employer employer : employers) {
                System.out.println(employer);
            }

            System.out.println("\nЗапрос для удаления: ");
            System.out.println(Employer.DELETE);
            firstEmployee = employers.get(5);
            secondEmployee = employers.get(7);
            Employer thirdEmployee = employers.get(1);

            firstEmployee.delete(connection);
            secondEmployee.delete(connection);
            thirdEmployee.delete(connection);

            System.out.println("\nЗаписи после удаления: ");
            employers = Employer.select(connection, query);
            for (Employer employer : employers) {
                System.out.println(employer);
            }

            System.out.println("\nФамилия сотрудника с максимальной зарплатой: ");
            try (CallableStatement statement = connection.prepareCall("{ ?= call getEmployeeLastNameWithMaxSalary() }")) {
                statement.registerOutParameter(1, Types.VARCHAR, 255);
                statement.execute();
                System.out.println(statement.getString(1));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static Properties loadProperties() throws IOException {
        Properties properties = new Properties();
        InputStream path = Main.class.getClassLoader().getResourceAsStream("db.properties");
        properties.load(path);

        return properties;
    }

    private static void createEmployersTable(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(Employer.DROP_TABLE);
        }
        try (Statement statement = connection.createStatement()) {
            statement.execute(Employer.CREATE);
            System.out.println("Таблица employers была создана.");
            System.out.println("Запрос для создания таблицы: \n" + Employer.CREATE);
        }
    }

    private static void createStoredFunction(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(Employer.DROP_FUNCTION_MAX_SALARY);
        }
        try (Statement statement = connection.createStatement()) {
            statement.execute(Employer.STORED_FUNCTION_MAX_SALARY);
            System.out.println("Хранимая функция getEmployeeLastNameWithMaxSalary была создана.");
            System.out.println("Запрос для создания хранимой функции: \n" + Employer.STORED_FUNCTION_MAX_SALARY);
        }
    }

    private static List<Employer> parseEmployersCSV(InputStream path) throws IOException {
        List<Employer> users = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(path, "UTF-8"))) {
            String line;
            while ((line = br.readLine()) != null) {
                users.add(Employer.fromCSVString(line));
            }
        }

        return users;
    }
}
