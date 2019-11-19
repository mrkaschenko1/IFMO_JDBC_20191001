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
import java.sql.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

public class DaoFactory {

    // private List<Employee> employees = getAllEmployees();
    // private List<Department> departments = getAllDepartments();

    private Connection createConnection() throws SQLException {
        ConnectionSource connectionSource = ConnectionSource.instance();
        Connection con = connectionSource.createConnection();
        System.out.println("connection established");
        return con;
    }

    private void close(Statement stmt) {
        try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
    }

    private void close(PreparedStatement stmt) {
        try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
    }

    private void close(Connection connection) {
        try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
    }

    public ResultSet getResultSetbySQL(String sql) {
        Connection con = null;
        Statement stmt = null;
        try {
            con = createConnection();
            stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            return rs;
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        } finally {
            close(stmt);
            close(con);
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
                Connection con = null;
                PreparedStatement prepStmt = null;
                try {
                    con = createConnection();
                    String sql = "SELECT * FROM EMPLOYEE WHERE DEPARTMENT=?";
                    prepStmt = con.prepareStatement(sql);
                    prepStmt.setInt(1, department.getId().intValue());
                    ResultSet rs = prepStmt.executeQuery();
                    while(rs.next()) {
                        employeesByDepartment.add(getEmployee(rs));
                    }
                    return employeesByDepartment;
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                } finally {
                    close(prepStmt);
                    close(con);
                }
            }

            @Override
            public List<Employee> getByManager(Employee employee) {
                List<Employee> employeesByManager = new LinkedList<>();
                Connection con = null;
                PreparedStatement prepStmt = null;
                try {
                    con = createConnection();
                    String sql = "SELECT * FROM EMPLOYEE WHERE MANAGER=?";
                    prepStmt = con.prepareStatement(sql);
                    prepStmt.setInt(1, employee.getId().intValue());
                    ResultSet rs = prepStmt.executeQuery();
                    while(rs.next()) {
                        employeesByManager.add(getEmployee(rs));
                    }
                    return employeesByManager;
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                } finally {
                    close(prepStmt);
                    close(con);
                }
            }

            @Override
            public Optional<Employee> getById(BigInteger Id) {
                Optional<Employee> employee = Optional.empty();
                Connection con = null;
                PreparedStatement prepStmt = null;
                try {
                    con = createConnection();
                    String sql = "SELECT * FROM EMPLOYEE WHERE ID=?";
                    prepStmt = con.prepareStatement(sql);
                    prepStmt.setInt(1, Id.intValue());
                    ResultSet rs = prepStmt.executeQuery();
                    if (rs.next()) {
                        employee = Optional.of(getEmployee(rs));
                    }
                    return employee;
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                } finally {
                    close(prepStmt);
                    close(con);
                }
            }

            @Override
            public List<Employee> getAll() {
                List<Employee> listEmployees = new LinkedList<>();
                try {
                    ResultSet rs = getResultSetbySQL("SELECT * FROM employee");
                    while (rs.next()) {
                        Employee employee = getEmployee(rs);
                        listEmployees.add(employee);
                    }
                    return listEmployees;
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                }
            }

            @Override
            public Employee save(Employee employee) {
                Connection con = null;
                PreparedStatement prepStmt = null;
                try {
                    con = createConnection();
                    String sql = "INSERT INTO employee VALUES (?,?,?,?,?,?,?,?,?)";
                    prepStmt = con.prepareStatement(sql);
                    prepStmt.setInt(1, employee.getId().intValue());
                    prepStmt.setString(2, employee.getFullName().getFirstName());
                    prepStmt.setString(3, employee.getFullName().getLastName());
                    prepStmt.setString(4, employee.getFullName().getMiddleName());
                    prepStmt.setString(5, employee.getPosition().toString());
                    prepStmt.setInt(6, employee.getManagerId().intValue());
                    prepStmt.setDate(7, Date.valueOf(employee.getHired()));
                    prepStmt.setDouble(8, employee.getSalary().doubleValue());
                    prepStmt.setInt(9, employee.getDepartmentId().intValue());

                    prepStmt.executeUpdate();
                    return employee;
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                } finally {
                    close(prepStmt);
                    close(con);
                }
            }

            @Override
            public void delete(Employee employee) {
                Connection con = null;
                PreparedStatement prepStmt = null;
                try {
                    con = createConnection();
                    String sql = "DELETE FROM EMPLOYEE WHERE ID=?";
                    prepStmt = con.prepareStatement(sql);
                    prepStmt.setInt(1, employee.getId().intValue());
                    prepStmt.executeUpdate();
                    return;
                } catch (SQLException e) {
                    e.printStackTrace();
                    return;
                } finally {
                    close(prepStmt);
                    close(con);
                }
            }
        };
    }

    public DepartmentDao departmentDAO() {
        return new DepartmentDao() {
            @Override
            public Optional<Department> getById(BigInteger Id) {
                Optional<Department> department = Optional.empty();
                Connection con = null;
                PreparedStatement prepStmt = null;
                try {
                    con = createConnection();
                    String sql = "SELECT * FROM DEPARTMENT WHERE ID=?";
                    prepStmt = con.prepareStatement(sql);
                    prepStmt.setInt(1, Id.intValue());
                    ResultSet rs = prepStmt.executeQuery();

                    if (rs.next()) {
                        department = Optional.of(getDepartment(rs));
                    }
                    return department;
                } catch (SQLException e) {
                    return Optional.empty();
                } finally {
                    close(prepStmt);
                    close(con);
                }
            }

            @Override
            public List<Department> getAll() {
                try {
                    List<Department> listDepartments = new LinkedList<>();
                    ResultSet rs = getResultSetbySQL("SELECT * FROM DEPARTMENT");
                    while (rs.next()) {
                        Department department = getDepartment(rs);
                        listDepartments.add(department);
                    }
                    return listDepartments;
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                }
            }

            @Override
            public Department save(Department department) {
                Connection con = null;
                PreparedStatement prepStmt = null;
                try {
                    con = createConnection();
                    String sql = null;
                    if (getById(department.getId()).isPresent()) {
                        sql = "UPDATE DEPARTMENT SET Name =?, Location =? WHERE Id =?";
                        prepStmt = con.prepareStatement(sql);
                        prepStmt.setInt(3, department.getId().intValue());
                        prepStmt.setString(1, department.getName());
                        prepStmt.setString(2, department.getLocation());
                    } else {
                        sql = "INSERT INTO DEPARTMENT VALUES (?, ?, ?)";
                        prepStmt = con.prepareStatement(sql);
                        prepStmt.setInt(1, department.getId().intValue());
                        prepStmt.setString(2, department.getName());
                        prepStmt.setString(3, department.getLocation());
                    }

                    prepStmt.executeUpdate();
                    return department;
                } catch (SQLException e) {
                    e.printStackTrace();
                    return null;
                } finally {
                    close(prepStmt);
                    close(con);
                }
            }

            @Override
            public void delete(Department department) {
                Connection con = null;
                PreparedStatement prepStmt = null;
                try {
                    con = createConnection();
                    String sql = "DELETE FROM DEPARTMENT WHERE ID=?";
                    prepStmt = con.prepareStatement(sql);
                    prepStmt.setString(1, department.getId().toString());
                    prepStmt.executeUpdate();
                    return;
                } catch (SQLException e) {
                    e.printStackTrace();
                    return;
                } finally {
                    close(prepStmt);
                    close(con);
                }
            }
        };
    }
}
