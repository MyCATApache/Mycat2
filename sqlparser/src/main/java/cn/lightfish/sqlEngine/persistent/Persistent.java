package cn.lightfish.sqlEngine.persistent;

import cn.lightfish.sqlEngine.schema.TableColumnDefinition;
import java.util.Map;

public interface Persistent {

  public InsertPersistent createInsertPersistent(TableColumnDefinition[] columnNameList,
      Map<String, Object> persistentAttributes);

  public QueryPersistent createQueryPersistent(TableColumnDefinition[] columnNameList,
      Map<String, Object> persistentAttributes);

  public UpdatePersistent createUpdatePersistent(TableColumnDefinition[] columnNameList, Map<String, Object> persistentAttributes);
}