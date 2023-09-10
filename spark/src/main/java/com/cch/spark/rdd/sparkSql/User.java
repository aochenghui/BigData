package com.cch.spark.rdd.sparkSql;
import java.io.Serializable;

public class User implements Serializable {
    private Long age;
    private String name;

    public User(Long age, String name) {
        this.age = age;
        this.name = name;
    }

    public User() {
    }

    public Long getAge() {
        return age;
    }

    public void setAge(Long age) {
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}