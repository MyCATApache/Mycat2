package io.mycat.sqlEngine.persistent;

import io.mycat.sqlEngine.schema.TableColumnDefinition;
import java.util.Map;

public interface Persistent {

  public InsertPersistent createInsertPersistent(TableColumnDefinition[] columnNameList,
      Map<String, Object> persistentAttributes);

  public QueryPersistent createQueryPersistent(TableColumnDefinition[] columnNameList,
      Map<String, Object> persistentAttributes);

  public UpdatePersistent createUpdatePersistent(TableColumnDefinition[] columnNameList, Map<String, Object> persistentAttributes);
}