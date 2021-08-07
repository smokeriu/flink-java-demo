package org.example.avro;

import java.io.Serializable;

public class User implements Serializable {
    String name;
    int favorite_number;
    String favorite_color;

    public User(String name, int favorite_number, String favorite_color) {
        this.name = name;
        this.favorite_number = favorite_number;
        this.favorite_color = favorite_color;
    }

    @Override
    public String toString() {
        return "User{" +
                "name='" + name + '\'' +
                ", favorite_number=" + favorite_number +
                ", favorite_color='" + favorite_color + '\'' +
                '}';
    }
}
