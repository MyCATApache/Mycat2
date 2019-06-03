package io.mycat.plug.sequence;

import io.mycat.beans.mycat.MycatTable;
import io.mycat.config.plug.PlugRootConfig;
import java.util.HashMap;
import java.util.Map;

public class SequenceManager {

  private final Map<String, SequenceHandler> map = new HashMap<>();

  public void load(PlugRootConfig rootConfig) {

  }

  public SequenceHandler getSequenceBySequenceName(MycatTable table) {
    return null;
  }
}