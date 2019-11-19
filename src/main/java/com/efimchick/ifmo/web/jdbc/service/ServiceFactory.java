package com.efimchick.ifmo.web.jdbc.service;

import com.efimchick.ifmo.web.jdbc.domain.Employee;
import com.efimchick.ifmo.web.jdbc.domain.Department;
import com.efimchick.ifmo.web.jdbc.domain.FullName;
import com.efimchick.ifmo.web.jdbc.domain.Position;
import com.efimchick.ifmo.web.jdbc.ConnectionSource;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.util.LinkedList;
import java.util.List;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

public class ServiceFactory {

        private Connection createConnection() throws SQLException {
            ConnectionSource connectionSource = ConnectionSource.instance();
            Connection con = connectionSource.createConnection();
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

        private void close(Connection connection) {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        public List<Employee> getAllEmployeesSorted(boolean chain, boolean managerNeeded, String sql) {
            Connection con = null;
            Statement stmt = null;
            try {
                List<Employee> listEmployees = new LinkedList<>();
                con = createConnection();
                stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                ResultSet resultSet = stmt.executeQuery(sql);
                while (resultSet.next()) {
                    Employee employee = getEmployee(resultSet, chain, managerNeeded);
                    listEmployees.add(employee);
                }
                return listEmployees;
            } catch (SQLException e) {
                e.printStackTrace();
                return null;
            } finally {
                close(stmt);
                close(con);
            }
        }


        public Department getDepartmentById(BigInteger id) {
            Connection con = null;
            Statement stmt = null;
            try {
                Department dep = null;
                con = createConnection();
                stmt = con.createStatement();
                ResultSet resultSet = stmt.executeQuery("SELECT * FROM DEPARTMENT where id=" + id);
                while (resultSet.next()) {
                    dep = getDepartment(resultSet);
                }
                return dep;
            } catch (SQLException e) {
                e.printStackTrace();
                return null;
            } finally {
                close(stmt);
                close(con);
            }
        }

        public Employee getEmployee(ResultSet rs, boolean chain, boolean managerNeeded) throws SQLException {
            BigInteger id = new BigInteger(rs.getString("id"));
            FullName fullName = new FullName(
                    rs.getString("firstName"),
                    rs.getString("lastName"),
                    rs.getString("middleName")
                    );
            Position position = Position.valueOf(rs.getString("position"));
            LocalDate date = LocalDate.parse(rs.getString("hireDate"));
            BigDecimal salary = rs.getBigDecimal("salary");
            Employee manager = null;
            if (rs.getObject("manager") != null) {
                if (chain || managerNeeded) {
                    BigInteger managerId = new BigInteger(rs.getString("manager"));
                    if (chain) {
                        manager = getAllEmployeesSorted(true, false, "SELECT * FROM employee WHERE id=" + managerId).get(0);
                    } else {
                        manager = getAllEmployeesSorted(false, false, "SELECT * FROM employee WHERE id=" + managerId).get(0);
                    }
                }
            }
            Department department = null;
            if (rs.getObject("department") != null) {
                BigInteger departmentId = BigInteger.valueOf(rs.getInt("department"));
                department = getDepartmentById(departmentId);
            }
            return new Employee(id, fullName, position, date, salary, manager, department);
        }

        public Department getDepartment(ResultSet rs) throws SQLException {
            BigInteger id = new BigInteger(rs.getString("id"));
            String name = rs.getString("name");
            String location = rs.getString("location");
            return new Department(id, name, location);
        }

        public List<Employee> getRequestedPage(String sql, Paging paging) {
            List<Employee> list = getAllEmployeesSorted(false, true, sql);
            int startIndex = Math.max((paging.page - 1) * paging.itemPerPage, 0);
            int endIndex = Math.min((paging.page) * paging.itemPerPage, list.size());
            return list.subList(startIndex, endIndex);
        }

        public EmployeeService employeeService(){
        return new EmployeeService() {

            @Override
            public List<Employee> getAllSortByHireDate(Paging paging) {
                return getRequestedPage("SELECT * FROM employee ORDER BY hireDate", paging);
            }

            @Override
            public List<Employee> getAllSortByLastname(Paging paging) {
                return getRequestedPage("SELECT * FROM employee ORDER BY lastName", paging);
            }

            @Override
            public List<Employee> getAllSortBySalary(Paging paging) {
                return getRequestedPage("SELECT * FROM employee ORDER BY salary", paging);
            }

            @Override
            public List<Employee> getAllSortByDepartmentNameAndLastname(Paging paging) {
                return getRequestedPage("SELECT * FROM employee ORDER BY department, lastname", paging);
            }

            @Override
            public List<Employee> getByDepartmentSortByHireDate(Department department, Paging paging) {
                return getRequestedPage("SELECT * FROM employee WHERE department=" + department.getId() + "ORDER BY hireDate", paging);
            }

            @Override
            public List<Employee> getByDepartmentSortBySalary(Department department, Paging paging) {
                return getRequestedPage("SELECT * FROM employee WHERE department=" + department.getId() + " ORDER BY salary", paging);
            }

            @Override
            public List<Employee> getByDepartmentSortByLastname(Department department, Paging paging) {
                return getRequestedPage("SELECT * FROM employee WHERE department=" + department.getId() + " ORDER BY lastName", paging);
            }

            @Override
            public List<Employee> getByManagerSortByLastname(Employee manager, Paging paging) {
                return getRequestedPage("SELECT * FROM employee WHERE manager=" + manager.getId() + " ORDER BY lastName", paging);
            }

            @Override
            public List<Employee> getByManagerSortByHireDate(Employee manager, Paging paging) {
                return getRequestedPage("SELECT * FROM employee WHERE manager=" + manager.getId() + " ORDER BY hireDate", paging);
            }

            @Override
            public List<Employee> getByManagerSortBySalary(Employee manager, Paging paging) {
                return getRequestedPage("SELECT * FROM employee WHERE manager=" + manager.getId() + " ORDER BY salary", paging);
            }

            @Override
            public Employee getWithDepartmentAndFullManagerChain(Employee employee) {
                return getAllEmployeesSorted(true, true, "SELECT * FROM employee WHERE id = " + employee.getId()).get(0);
            }

            @Override
            public Employee getTopNthBySalaryByDepartment(int salaryRank, Department department) {
                return getAllEmployeesSorted(false, true, "SELECT * FROM employee WHERE department=" + department.getId() + " ORDER BY salary DESC").get(salaryRank-1);
            }
        };
    }
}
