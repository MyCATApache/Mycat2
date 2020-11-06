package io.mycat.hbt4;

import lombok.Data;

import java.util.LinkedList;
import java.util.List;

@Data
public class Group {
    public LinkedList<List<Object>> args = new LinkedList<>();
}
