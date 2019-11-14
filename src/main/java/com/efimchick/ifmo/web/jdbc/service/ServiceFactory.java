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
import java.util.Comparator;
import java.util.Collections;
//import java.util.Optional;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

public class ServiceFactory {

        private List<Department> departments = getAllDepartments();
        private List<Employee> employeesChain = getAllEmployees(true);
        private List<Employee> employeesNoChain= getAllEmployees(false);

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

        private void close(Connection connection) {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        public List<Employee> getAllEmployees(boolean chain) {
            Connection con = null;
            Statement stmt = null;
            try {
                List<Employee> listEmployees = new LinkedList<>();
                con = createConnection();
                stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                ResultSet resultSet = stmt.executeQuery("SELECT * FROM EMPLOYEE");
                while (resultSet.next()) {
                    Employee employee = getEmployee(resultSet, chain, true);
                    listEmployees.add(employee);
                }
                return listEmployees;
            } catch (SQLException e) {
                //System.out.println(e.getMessage());
                e.printStackTrace();
                return null;
            } finally {
                close(stmt);
                close(con);
            }
        }

        public List<Department> getAllDepartments() {
            Connection con = null;
            Statement stmt = null;
            try {
                List<Department> listDepartments = new LinkedList<>();
                con = createConnection();
                stmt = con.createStatement();
                ResultSet resultSet = stmt.executeQuery("SELECT * FROM DEPARTMENT");
                while (resultSet.next()) {
                    Department department = getDepartment(resultSet);
                    listDepartments.add(department);
                }
                return listDepartments;
            } catch (SQLException e) {
                //System.out.println(e.getMessage());
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
                    int currentRow = rs.getRow();
                    rs.beforeFirst();
                    while (rs.next()) {
                        if (rs.getInt("id") == managerId.intValue()) {
                            manager = getEmployee(rs, chain, false);
                        }
                    }
                    rs.absolute(currentRow);
                }
            }
            Department department = null;
            if (rs.getObject("department") != null) {
                BigInteger departmentId = BigInteger.valueOf(rs.getInt("department"));
                for (Department dep: departments) {
                    if (dep.getId().equals(departmentId)) {
                        department = dep;
                    }
                }

            }
            return new Employee(id, fullName, position, date, salary, manager, department);
        }

        public Department getDepartment(ResultSet rs) throws SQLException {
            BigInteger id = new BigInteger(rs.getString("id"));
            String name = rs.getString("name");
            String location = rs.getString("location");
            return new Department(id, name, location);
        }

        public List<Employee> getByDep(Department dep) {
            List<Employee> listByDep = new LinkedList<>();
            for (Employee emp: employeesNoChain) {
                if (emp.getDepartment() != null && emp.getDepartment().getId().equals(dep.getId())) {
                    listByDep.add(emp);
                }
            }
            return listByDep;
        }

        public List<Employee> getByManager(Employee mng) {
            List<Employee> listByMng = new LinkedList<>();
            for (Employee emp: employeesNoChain) {
                if (emp.getManager() != null && emp.getManager().getId().equals(mng.getId())) {
                    listByMng.add(emp);
                }
            }
            return listByMng;
        }

        public EmployeeService employeeService(){
        return new EmployeeService() {

            @Override
            public List<Employee> getAllSortByHireDate(Paging paging) {
                Collections.sort(employeesNoChain, new Comparator<Employee>() {
                    public int compare(Employee emp1, Employee emp2) {
                        int hireDateComp = emp1.getHired().compareTo(emp2.getHired());
                        return hireDateComp;
                    }
                });
                int startIndex = Math.max((paging.page - 1) * paging.itemPerPage, 0);
                int endIndex = Math.min((paging.page) * paging.itemPerPage, employeesNoChain.size());
                return employeesNoChain.subList(startIndex, endIndex);
            }

            @Override
            public List<Employee> getAllSortByLastname(Paging paging) {
                Collections.sort(employeesNoChain, new Comparator<Employee>() {
                    public int compare(Employee emp1, Employee emp2) {
                        int lastNameComp = emp1.getFullName().getLastName().compareTo(emp2.getFullName().getLastName());
                        return lastNameComp;
                    }
                });
                int startIndex = Math.max((paging.page - 1) * paging.itemPerPage, 0);
                int endIndex = Math.min((paging.page) * paging.itemPerPage, employeesNoChain.size());
                return employeesNoChain.subList(startIndex, endIndex);
            }

            @Override
            public List<Employee> getAllSortBySalary(Paging paging) {
                Collections.sort(employeesNoChain, new Comparator<Employee>() {
                    public int compare(Employee emp1, Employee emp2) {
                        int salaryComp = emp1.getSalary().compareTo(emp2.getSalary());
                        return salaryComp;
                    }
                });
                int startIndex = Math.max((paging.page - 1) * paging.itemPerPage, 0);
                int endIndex = Math.min((paging.page) * paging.itemPerPage, employeesNoChain.size());
                return employeesNoChain.subList(startIndex, endIndex);
            }

            @Override
            public List<Employee> getAllSortByDepartmentNameAndLastname(Paging paging) {
                Collections.sort(employeesNoChain, new Comparator<Employee>() {
                    public int compare(Employee emp1, Employee emp2) {
                        if (emp1.getDepartment() == null) {
                            return -1;
                        } else if (emp2.getDepartment() == null) {
                            return 1;
                        }
                        int depComp = emp1.getDepartment().getName().compareTo(emp2.getDepartment().getName());
                        if (depComp != 0) {
                            return depComp;
                        }
                        int lastNameComp = emp1.getFullName().getLastName().compareTo(emp2.getFullName().getLastName());
                        return lastNameComp;
                    }
                });
                int startIndex = Math.max((paging.page - 1) * paging.itemPerPage, 0);
                int endIndex = Math.min((paging.page) * paging.itemPerPage, employeesNoChain.size());
                return employeesNoChain.subList(startIndex, endIndex);
            }

            @Override
            public List<Employee> getByDepartmentSortByHireDate(Department department, Paging paging) {
                List<Employee> listByDep = getByDep(department);
                Collections.sort(listByDep, new Comparator<Employee>() {
                    public int compare(Employee emp1, Employee emp2) {
                        int hireDateComp = emp1.getHired().compareTo(emp2.getHired());
                        return hireDateComp;
                    }
                });
                int startIndex = Math.max((paging.page - 1) * paging.itemPerPage, 0);
                int endIndex = Math.min((paging.page) * paging.itemPerPage, listByDep.size());
                return listByDep.subList(startIndex, endIndex);
            }

            @Override
            public List<Employee> getByDepartmentSortBySalary(Department department, Paging paging) {
                List<Employee> listByDep = getByDep(department);
                Collections.sort(listByDep, new Comparator<Employee>() {
                    public int compare(Employee emp1, Employee emp2) {
                        int salaryComp = emp1.getSalary().compareTo(emp2.getSalary());
                        return salaryComp;
                    }
                });
                int startIndex = Math.max((paging.page - 1) * paging.itemPerPage, 0);
                int endIndex = Math.min((paging.page) * paging.itemPerPage, listByDep.size());
                return listByDep.subList(startIndex, endIndex);
            }

            @Override
            public List<Employee> getByDepartmentSortByLastname(Department department, Paging paging) {
                List<Employee> listByDep = getByDep(department);
                Collections.sort(listByDep, new Comparator<Employee>() {
                    public int compare(Employee emp1, Employee emp2) {
                        int lastNameComp = emp1.getFullName().getLastName().compareTo(emp2.getFullName().getLastName());
                        return lastNameComp;
                    }
                });
                int startIndex = Math.max((paging.page - 1) * paging.itemPerPage, 0);
                int endIndex = Math.min((paging.page) * paging.itemPerPage, listByDep.size());
                return listByDep.subList(startIndex, endIndex);
            }

            @Override
            public List<Employee> getByManagerSortByLastname(Employee manager, Paging paging) {
                List<Employee> listByMng = getByManager(manager);
                Collections.sort(listByMng, new Comparator<Employee>() {
                    public int compare(Employee emp1, Employee emp2) {
                        int lastNameComp = emp1.getFullName().getLastName().compareTo(emp2.getFullName().getLastName());
                        return lastNameComp;
                    }
                });
                int startIndex = Math.max((paging.page - 1) * paging.itemPerPage, 0);
                int endIndex = Math.min((paging.page) * paging.itemPerPage, listByMng.size());
                return listByMng.subList(startIndex, endIndex);
            }

            @Override
            public List<Employee> getByManagerSortByHireDate(Employee manager, Paging paging) {
                List<Employee> listByMng = getByManager(manager);
                Collections.sort(listByMng, new Comparator<Employee>() {
                    public int compare(Employee emp1, Employee emp2) {
                        int hireDateComp = emp1.getHired().compareTo(emp2.getHired());
                        return hireDateComp;
                    }
                });
                int startIndex = Math.max((paging.page - 1) * paging.itemPerPage, 0);
                int endIndex = Math.min((paging.page) * paging.itemPerPage, listByMng.size());
                return listByMng.subList(startIndex, endIndex);
            }

            @Override
            public List<Employee> getByManagerSortBySalary(Employee manager, Paging paging) {
                List<Employee> listByMng = getByManager(manager);
                Collections.sort(listByMng, new Comparator<Employee>() {
                    public int compare(Employee emp1, Employee emp2) {
                        int salaryComp = emp1.getSalary().compareTo(emp2.getSalary());
                        return salaryComp;
                    }
                });
                int startIndex = Math.max((paging.page - 1) * paging.itemPerPage, 0);
                int endIndex = Math.min((paging.page) * paging.itemPerPage, listByMng.size());
                return listByMng.subList(startIndex, endIndex);
            }

            @Override
            public Employee getWithDepartmentAndFullManagerChain(Employee employee) {
                for (Employee emp: employeesChain) {
                    if (emp.getId().equals(employee.getId())) {
                        return emp;
                    }
                }
                return null;
            }

            @Override
            public Employee getTopNthBySalaryByDepartment(int salaryRank, Department department) {
                List<Employee> listByDep = getByDep(department);
                Collections.sort(listByDep, new Comparator<Employee>() {
                    public int compare(Employee emp1, Employee emp2) {
                        int salaryComp = (emp1.getSalary().compareTo(emp2.getSalary()))*(-1); //descending order
                        return salaryComp;
                    }
                });
                return listByDep.get(salaryRank-1);
            }
        };
    }
}
