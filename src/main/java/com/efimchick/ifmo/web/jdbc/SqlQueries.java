package com.efimchick.ifmo.web.jdbc;

/**
 * Implement sql queries like described
 */
public class SqlQueries {
    //Select all employees sorted by last name in ascending order
    //language=HSQLDB
    String select01 = "SELECT * FROM EMPLOYEE ORDER BY lastname";

    //Select employees having no more than 5 characters in last name sorted by last name in ascending order
    //language=HSQLDB
    String select02 = "SELECT * FROM EMPLOYEE WHERE LENGTH(lastname) < 6 ORDER BY lastname";

    //Select employees having salary no less than 2000 and no more than 3000
    //language=HSQLDB
    String select03 = "SELECT * FROM EMPLOYEE WHERE salary BETWEEN 2000 AND 3000";

    //Select employees having salary no more than 2000 or no less than 3000
    //language=HSQLDB
    String select04 = "SELECT * FROM EMPLOYEE WHERE salary <= 2000 OR salary >= 3000";

    //Select employees assigned to a department and corresponding department name
    //language=HSQLDB
    String select05 = "SELECT EMPLOYEE.lastname, EMPLOYEE.salary, DEPARTMENT.name " +
                        "FROM EMPLOYEE INNER JOIN DEPARTMENT ON EMPLOYEE.department = DEPARTMENT.id";

    //Select all employees and corresponding department name if there is one.
    //Name column containing name of the department "depname".
    //language=HSQLDB
    String select06 = "SELECT EMPLOYEE.lastname, EMPLOYEE.salary, DEPARTMENT.name AS depname " +
                        "FROM EMPLOYEE LEFT JOIN DEPARTMENT ON EMPLOYEE.department = DEPARTMENT.id";

    //Select total salary pf all employees. Name it "total".
    //language=HSQLDB
    String select07 = "SELECT SUM(salary) AS total FROM EMPLOYEE";

    //Select all departments and amount of employees assigned per department
    //Name column containing name of the department "depname".
    //Name column containing employee amount "staff_size".
    //language=HSQLDB
    String select08 = "SELECT DEPARTMENT.name AS depname, COUNT(EMPLOYEE.id) AS staff_size " +
     "FROM DEPARTMENT INNER JOIN EMPLOYEE ON EMPLOYEE.department = DEPARTMENT.id " +
     "GROUP BY depname";

    //Select all departments and values of total and average salary per department
    //Name column containing name of the department "depname".
    //language=HSQLDB
    String select09 = "SELECT DEPARTMENT.name AS depname, SUM(EMPLOYEE.salary) AS total, AVG(EMPLOYEE.salary) AS average " +
     "FROM DEPARTMENT JOIN EMPLOYEE ON EMPLOYEE.department = DEPARTMENT.id " +
     "GROUP BY depname ORDER BY total DESC";

    //Select all employees and their managers if there is one.
    //Name column containing employee lastname "employee".
    //Name column containing manager lastname "manager".
    //language=HSQLDB
    String select10 = "SELECT Emp1.lastname AS employee, Emp2.lastname AS manager FROM EMPLOYEE EMP1 LEFT JOIN EMPLOYEE EMP2 ON EMP1.manager=EMP2.id";


}
