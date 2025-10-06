package skycomposer.flinkiceberg;

public class EmployeeData {
    private Long id;
    private String department;
    private Long salary;

    public EmployeeData() {
    }

    public EmployeeData(Long id, String department, Long salary) {
        this.id = id;
        this.department = department;
        this.salary = salary;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getDepartment() {
        return department;
    }

    public void setDepartment(String department) {
        this.department = department;
    }

    public Long getSalary() {
        return salary;
    }

    public void setSalary(Long salary) {
        this.salary = salary;
    }
}
