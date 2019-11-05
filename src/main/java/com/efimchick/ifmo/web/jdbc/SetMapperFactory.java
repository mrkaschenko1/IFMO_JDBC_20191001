package com.efimchick.ifmo.web.jdbc;

import java.util.Set;
import java.util.HashSet;
import java.sql.SQLException;
import java.sql.ResultSet;
import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;
import java.math.BigInteger;
import java.math.BigDecimal;
import java.time.LocalDate;


public class SetMapperFactory {

    public SetMapper<Set<Employee>> employeesSetMapper() {
        return new SetMapper<Set<Employee>>() {
           @Override
           public Set<Employee> mapSet(ResultSet resultSet) {
               try {
                   Set<Employee> employees = new HashSet<Employee>();
                   while (resultSet.next()) {
                       Employee employee = getEmployee(resultSet);
                       employees.add(employee);
                   }
                   return employees;
               } catch (SQLException e) {
                   e.printStackTrace();
                   return null;
               }
           }
       };
    }
    private static Employee getEmployee(ResultSet resultSet) throws SQLException {
        try {
            // int currentRow = resultSet.getRow();
            // int managerId = resultSet.getInt("manager");
            Employee manager = null;
            Employee employee;

            BigInteger id = new BigInteger(resultSet.getString("id"));
            FullName fullName = new FullName(
                    resultSet.getString("firstName"),
                    resultSet.getString("lastName"),
                    resultSet.getString("middleName")
                    );
            Position position = Position.valueOf(resultSet.getString("position"));
            LocalDate date = LocalDate.parse(resultSet.getString("hireDate"));
            BigDecimal salary = resultSet.getBigDecimal("salary");

            if (resultSet.getString("manager") != null) {
                int managerId = Integer.valueOf(resultSet.getString("manager"));
                int currentRow = resultSet.getRow();
                resultSet.beforeFirst();
                while (resultSet.next() && manager == null) {
                    if (resultSet.getInt("ID") == managerId) {
                        manager = getEmployee(resultSet);
                    }
                }
                resultSet.absolute(currentRow);
            }
            return new Employee(id, fullName, position, date, salary, manager);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
}
