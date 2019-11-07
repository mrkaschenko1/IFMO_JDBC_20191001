package com.efimchick.ifmo.web.jdbc.dao;

import com.efimchick.ifmo.web.jdbc.ConnectionSource;
import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.Department;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class DaoFactory {

    private List<Employee> employees = getAllEmployees();
    private List<Department> departments = getAllDepartments();

    private Connection createConnection() throws SQLException {
        ConnectionSource connectionSource = ConnectionSource.instance();
        Connection con = connectionSource.createConnection();
        System.out.println("connection established");
        return con;
    }

    private void close(Statement st) throws SQLException {
        if (st != null) {
            st.close();
        }
    }

    private void close(Connection connection) throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    public List<Employee> getAllEmployees() {
        try {
            List<Employee> listEmployees = new LinkedList<>();
            Connection con = createConnection();
            Statement st = con.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM EMPLOYEE");            while (rs.next()) {
                Employee employee = getEmployee(rs);
                listEmployees.add(employee);
            }
            close(st);
            close(con);
            return listEmployees;
        } catch (SQLException e) {
            //System.out.println(e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    public List<Department> getAllDepartments() {
        try {
            List<Department> listDepartments = new LinkedList<>();
            Connection con = createConnection();
            Statement st = con.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM DEPARTMENT");
            while (rs.next()) {
                Department department = getDepartment(rs);
                listDepartments.add(department);
            }
            close(st);
            close(con);
            return listDepartments;
        } catch (SQLException e) {
            //System.out.println(e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    public Employee getEmployee(ResultSet rs) throws SQLException {
        BigInteger id = new BigInteger(rs.getString("id"));
        FullName fullName = new FullName(
                rs.getString("firstName"),
                rs.getString("lastName"),
                rs.getString("middleName")
                );
        Position position = Position.valueOf(rs.getString("position"));
        LocalDate date = LocalDate.parse(rs.getString("hireDate"));
        BigDecimal salary = rs.getBigDecimal("salary");
        BigInteger manager = null;
        if (rs.getObject("manager") != null) {
            manager = new BigInteger(rs.getString("manager"));
        } else {
            manager = BigInteger.valueOf(0);
        }
        BigInteger department = null;
        if (rs.getObject("department") != null) {
            department = new BigInteger(rs.getString("department"));
        } else {
            department = BigInteger.valueOf(0);
        }
        return new Employee(id, fullName, position, date, salary, manager, department);
    }

    public Department getDepartment(ResultSet rs) throws SQLException {
        BigInteger id = new BigInteger(rs.getString("id"));
        String name = rs.getString("name");
        String location = rs.getString("location");
        return new Department(id, name, location);
    }

    public EmployeeDao employeeDAO() {
        return new EmployeeDao() {
            @Override
            public List<Employee> getByDepartment(Department department) {
                List<Employee> employeesByDepartment = new LinkedList<>();
                for(Employee emp: employees) {
                    if (emp.getDepartmentId().equals(department.getId())) {
                        employeesByDepartment.add(emp);
                    }
                }
                return employeesByDepartment;
            }

            @Override
            public List<Employee> getByManager(Employee employee) {
                List<Employee> employeesByManager = new LinkedList<>();
                for(Employee emp: employees) {
                    if (emp.getManagerId().equals(employee.getId())) {
                        employeesByManager.add(emp);
                    }
                }
                return employeesByManager;
            }

            @Override
            public Optional<Employee> getById(BigInteger Id) {
                Optional<Employee> employee = Optional.empty();
                for (int i = 0; i < employees.size(); i++) {
                    if (employees.get(i).getId().equals(Id)) {
                        employee = Optional.of(employees.get(i));
                        break;
                    }
                }
                return employee;
            }

            @Override
            public List<Employee> getAll() {
                return employees;
            }

            @Override
            public Employee save(Employee employee) {
                employees.add(employee);
                return employee;
            }

            @Override
            public void delete(Employee employee) {
                employees.remove(employee);
            }
        };
    }

    public DepartmentDao departmentDAO() {
        return new DepartmentDao() {
            @Override
            public Optional<Department> getById(BigInteger Id) {
                Optional<Department> department = Optional.empty();
                for(Department dep: departments) {
                    if (dep.getId().equals(Id)) {
                        department = Optional.of(dep);
                    }
                }
                return department;
            }

            @Override
            public List<Department> getAll() {
                return departments;
            }

            @Override
            public Department save(Department department) {
                for(Department dep: departments) {
                    if (dep.getId().equals(department.getId())) { //check existed
                        departments.remove(dep);
                    }
                }
                departments.add(department);
                return department;
            }

            @Override
            public void delete(Department department) {
                departments.remove(department);
            }
        };
    }
}
