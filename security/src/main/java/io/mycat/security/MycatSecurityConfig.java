package io.mycat.security;

import io.mycat.MycatExpection;
import io.mycat.beans.mycat.MycatSchema;
import io.mycat.beans.mycat.MycatTable;
import io.mycat.beans.mysql.MySQLErrorCode;
import io.mycat.config.user.UserConfig;
import io.mycat.config.user.UserRootConfig;
import io.mycat.router.MycatRouterConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * @author jamie12221
 * @date 2019-05-18 19:40
 **/
public class MycatSecurityConfig {

  private UserRootConfig config;
  private MycatRouterConfig routerConfig;
  private Map<String, UserConfig> users = new HashMap<>();

  public MycatSecurityConfig(UserRootConfig config, MycatRouterConfig routerConfig) {
    this.config = config;
    this.routerConfig = routerConfig;
    for (UserConfig user : config.getUsers()) {
      Objects.requireNonNull(user);
      users.put(user.getName(), user);
    }
  }

  public String getPasswordByUserName(String userName) {
    UserConfig userConfig = users.get(userName);
    if (userConfig == null) {
      throw new MycatExpection("not exist user name" + userName);
    }
    String password = users.get(userName).getPassword();
    if (password == null) {
      return "";
    }
    return password;
  }


  public boolean isIgnorePassword() {
    return false;
  }


  public MycatUser getUser(String host, String userName, int packetMaxSize) throws MycatExpection {
    UserConfig userConfig = users.get(userName);
    HashMap<String, MycatSchema> map = new HashMap<>();
    for (String schemaName : userConfig.getSchemas()) {
      MycatSchema schema = routerConfig.getSchemaBySchemaName(schemaName);
      if (schema == null) {
        continue;
      }
      map.put(schema.getSchemaName(), schema);
    }
    return new UserImpl(userName, host, map, packetMaxSize);
  }


  public String interceptSQL(String sql, int sqlType) {
    return sql;
  }

  public int checkSchema(MycatUser userBean, String schema) {
    if (schema == null) {
      return 0;
    }
    Map<String, MycatSchema> schemas = userBean.getSchemas();
    if (schemas == null || schemas.size() == 0 || schemas.containsKey(schema)) {
      return 0;
    } else {
      return MySQLErrorCode.ER_DBACCESS_DENIED_ERROR;
    }
  }

  private class UserImpl implements MycatUser {

    String userName;
    String host;
    HashMap<String, MycatSchema> schemas;
    int maxPacketSize;

    public UserImpl(String userName, String host,
        HashMap<String, MycatSchema> schemas, int maxPacketSize) {
      this.userName = userName;
      this.host = host;
      this.schemas = schemas;
      this.maxPacketSize = maxPacketSize;
    }

    @Override
    public boolean checkMaxPacketSize(int packetSize) {
      return maxPacketSize < packetSize;
    }

    public String getUserName() {
      return userName;
    }

    public String getHost() {
      return host;
    }

    public Map<String, MycatSchema> getSchemas() {
      return schemas;
    }

    @Override
    public boolean existSchema(String schemaName) {
      return schemas.containsKey(schemaName);
    }

    @Override
    public boolean checkSQL(int sqltype, String sql, Set<MycatTable> table) {
      return true;
    }

  }
}
