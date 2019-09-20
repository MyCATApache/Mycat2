package cn.lightfish.sqlEngine.context;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.repository.SchemaRepository;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
/**
 * @author Junwen Chen
 **/
public enum GlobalContext {
  INSTANCE;
  final ConcurrentMap<String, Object> map = new ConcurrentHashMap<>();
  public final SchemaRepository CACHE_REPOSITORY = new SchemaRepository(DbType.mysql);

  public Object getGlobalVariant(String name) {
    return map.get(name);
  }
}