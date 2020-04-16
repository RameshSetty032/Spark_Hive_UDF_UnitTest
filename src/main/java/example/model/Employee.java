package example.model;

import java.io.Serializable;

public class Employee implements Serializable {
        private String name;
        private long salary;

        // Constructors, getters, setters...
        // $example off:typed_custom_aggregation$
        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getSalary() {
            return salary;
        }

        public void setSalary(long salary) {
            this.salary = salary;
        }
        // $example on:typed_custom_aggregation$
    }


