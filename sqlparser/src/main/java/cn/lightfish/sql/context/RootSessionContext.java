package cn.lightfish.sql.context;

import java.util.HashMap;
import java.util.Map;

public class RootSessionContext {

  public Object[] scope = new Object[32];
  public final Map<String, Object> sessionVariants = new HashMap<>();

  public RootSessionContext() {
  }


  public void createTableSourceContext(int size) {
    scope = new Object[size];
  }

  public Object getGlobalVariant(String name) {
    return GlobalContext.INSTANCE.getGlobalVariant(name);
  }

  public Object getSessionVariant(String name) {
    return sessionVariants.get(name);
  }
}