package io.mycat.calcite.executor;

import lombok.Data;

import java.util.LinkedList;
import java.util.List;

@Data
public class Group {
    public LinkedList<List<Object>> args = new LinkedList<>();
}
