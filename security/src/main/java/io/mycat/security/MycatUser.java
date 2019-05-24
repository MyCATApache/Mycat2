package io.mycat.security;

import io.mycat.beans.mycat.MycatSchema;
import io.mycat.beans.mycat.MycatTable;
import java.util.Map;
import java.util.Set;

public interface MycatUser {

  boolean checkMaxPacketSize(int packetSize);

  String getUserName();

  String getHost();

  Map<String, MycatSchema> getSchemas();

  boolean existSchema(String schemaName);

  boolean checkSQL(int sqltype, String sql, Set<MycatTable> table);
}
