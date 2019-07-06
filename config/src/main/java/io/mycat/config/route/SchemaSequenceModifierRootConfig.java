package io.mycat.config.route;

import io.mycat.config.ConfigurableRoot;
import io.mycat.config.YamlUtil;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SchemaSequenceModifierRootConfig extends ConfigurableRoot {

  Map<String, SequenceModifierConfig> modifiers;

  public static void main(String[] args) {
    SchemaSequenceModifierRootConfig root = new SchemaSequenceModifierRootConfig();
    HashMap<String, SequenceModifierConfig> configHashMap = new HashMap<>();
    SequenceModifierConfig sequenceModifierConfig = new SequenceModifierConfig();
    configHashMap.put("ANNOTATION_ROUTE", sequenceModifierConfig);
    sequenceModifierConfig
        .setSequenceModifierClazz("io.mycat.router.sequence.SequenceModifierImpl");
    HashMap<String, String> properties = new HashMap<>();
    sequenceModifierConfig.setSequenceModifierProperties(properties);
    properties.put("sequenceHandlerClass", "io.mycat.router.sequence.MySQLSequenceHandlerImpl");
    properties.put("pattern", "(?:(\\s*next\\s+value\\s+for\\s*MYCATSEQ_(\\w+))(,|\\)|\\s)*)+");
    properties.put("mysqlSeqTimeout", Long.toString(TimeUnit.SECONDS.toMillis(1)));
    properties.put("mysqlSeqSource-ANNOTATION_ROUTE-travelrecord", "mytest3306-db1-GLOBAL");
    root.setModifiers(configHashMap);
    System.out.println(YamlUtil.dump(root));
  }

  public Map<String, SequenceModifierConfig> getModifiers() {
    return modifiers;
  }

  public void setModifiers(
      Map<String, SequenceModifierConfig> modifiers) {
    this.modifiers = modifiers;
  }


}