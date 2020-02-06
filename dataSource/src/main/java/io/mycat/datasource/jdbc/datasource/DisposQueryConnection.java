package io.mycat.datasource.jdbc.datasource;

import java.util.*;

public class DisposQueryConnection implements AutoCloseable {
    final Map<String, LinkedList<DefaultConnection>> map;
    private final List<LinkedList<DefaultConnection>> connections;

    public DisposQueryConnection(Map<String, LinkedList<DefaultConnection>> map) {
        this.map = map;
        this.connections = new ArrayList<>(this.map.values());
    }

    @Override
    public void close() throws Exception {
        map.values().stream().flatMap(Collection::stream).forEach(i -> i.close());
    }

    public DefaultConnection get(String jdbcDataSource) {
        LinkedList<DefaultConnection> defaultConnections = map.get(jdbcDataSource);
        return defaultConnections.remove();
    }

}