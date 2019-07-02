package io.mycat.security;

import io.mycat.MycatException;
import io.mycat.config.user.UserConfig;
import io.mycat.config.user.UserRootConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author jamie12221
 *  date 2019-05-18 19:40
 **/
public class MycatSecurityConfig {
  private UserRootConfig config;
  private Map<String, UserConfig> users = new HashMap<>();
  private static final Logger LOGGER = LoggerFactory.getLogger(MycatSecurityConfig.class);
  public MycatSecurityConfig(UserRootConfig config) {
    this.config = config;
    Objects.requireNonNull(config);
    ////////////////////////////////////check/////////////////////////////////////////////////
    Objects.requireNonNull(config.getUsers(), "user config can not be empty");
    ////////////////////////////////////check/////////////////////////////////////////////////
    for (UserConfig user : config.getUsers()) {
      ////////////////////////////////////check/////////////////////////////////////////////////
      Objects.requireNonNull(user.getName(), "user name can not be empty");
      ////////////////////////////////////check/////////////////////////////////////////////////
      users.put(user.getName(), user);
    }
  }

  public String getPasswordByUserName(String userName) {
    UserConfig userConfig = users.get(userName);
    if (userConfig == null) {
      throw new MycatException("not exist user name" + userName);
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


  public MycatUser getUser(String host, String userName) throws MycatException {
    UserConfig userConfig = users.get(userName);
    return new UserImpl(userName, host);
  }


  public String interceptSQL(String sql, int sqlType) {
    return sql;
  }


  private class UserImpl implements MycatUser {

    String userName;
    String host;

    public UserImpl(String userName, String host) {
      this.userName = userName;
      this.host = host;
    }


    public String getUserName() {
      return userName;
    }

    public String getHost() {
      return host;
    }

  }
}
