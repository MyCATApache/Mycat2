package io.mycat.wu.ast;

public enum Direction {
    ASC("asc"),
    DESC("desc");
    String name;

    Direction(String name) {
        this.name = name;
    }

    public static Direction parse(String value) {
        return Direction.valueOf(value.toUpperCase());
    }

    public String getName() {
        return name;
    }

}

