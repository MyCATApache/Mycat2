package io.mycat.calcite;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.mycat.MetaClusterCurrent;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.replica.ReplicaSelectorManager;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.Objects;

public class UnsolvedMysqlFunctionUtil {
    public static Cache<String, Object> objectCache = (Cache) (CacheBuilder.newBuilder().maximumSize(65535).build());

    public  static interface Fun{
        public  abstract Object eval( Object... args) ;
    }
    @SneakyThrows
    public static Object eval(String fun, Object... args){
        ArrayList<String> p = new ArrayList<>(args.length);
        for (Object arg : args) {
            if (arg == null) {
                p.add("null");
            } else if (arg instanceof String) {
                p.add("'" + arg + "'");
            } else {
                p.add(Objects.toString(arg));
            }
        }
        String sql = "select " + fun + "(" + String.join(",", p) + ")";
        return objectCache.get(sql, () -> {
            ReplicaSelectorManager replicaSelectorRuntime = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
            JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
            String datasource = replicaSelectorRuntime.getDatasourceNameByRandom();
            try (DefaultConnection connection = jdbcConnectionManager.getConnection(datasource)) {
                RowBaseIterator rowBaseIterator = connection.executeQuery(sql);
                rowBaseIterator.next();
                return rowBaseIterator.getObject(1);
            }
        });
    }
    @SneakyThrows
    public static Object eval(String fun, Object args) {
        if (args instanceof Object[]){
            return eval(fun,(Object[]) args);
        }else {
            return eval(fun,new Object[]{args});
        }

    }
}