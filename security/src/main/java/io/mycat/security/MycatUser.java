package io.mycat.security;

import io.mycat.beans.mycat.MycatSchema;
import io.mycat.beans.mycat.MycatTable;
import java.util.Map;
import java.util.Set;

public interface MycatUser {
  String getUserName();

  String getHost();
}
