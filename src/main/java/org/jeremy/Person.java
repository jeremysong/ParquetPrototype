package org.jeremy;

import com.google.common.base.MoreObjects;

import java.util.List;

public class Person {

    private String name;
    private int age;
    private List<String> pets;

    public Person(String name, int age, List<String> pets) {
        this.name = name;
        this.age = age;
        this.pets = pets;
    }

    public String getName() {
        return name;
    }

    public int getAge() {
        return age;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("Name", name).add("Age", age).toString();
    }
}
